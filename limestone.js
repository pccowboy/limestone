var tcp = require('net');
var sys = require('sys');
var events = require('events');
var Step = require("step");

var limestone = exports;

    var buffer_extras = require('./buffer_extras');

    var _defaults = {
    	host				: 'localhost',
    	port				: 9312,
    	connection_mode		: 'PERSIST'
    };

    limestone.Sphinx = {};

    // All search modes
    Sphinx.searchMode = {
        "ALL":0,
        "ANY":1,
        "PHRASE":2,
        "BOOLEAN":3,
        "EXTENDED":4,
        "FULLSCAN":5,
        "EXTENDED2":6    // extended engine V2 (TEMPORARY, WILL BE REMOVED)
    };

    // All ranking modes
    Sphinx.rankingMode = {
        "PROXIMITY_BM25"	: 0,    ///< default mode, phrase proximity major factor and BM25 minor one
        "BM25"				: 1,    ///< statistical mode, BM25 ranking only (faster but worse quality)
        "NONE"				: 2,    ///< no ranking, all matches get a weight of 1
        "WORDCOUNT"			: 3,    ///< simple word-count weighting, rank is a weighted sum of per-field keyword occurence counts
        "PROXIMITY"			: 4,
        "MATCHANY"			: 5,
        "FIELDMASK"			: 6,
        "SPH04"				: 7,
        "TOTAL"				: 8
    };

    Sphinx.sortMode = {
        "RELEVANCE"		: 0,
        "ATTR_DESC"		: 1,
        "ATTR_ASC"		: 2,
        "TIME_SEGMENTS"	: 3,
        "EXTENDED"		: 4,
        "EXPR"			: 5
    };

    Sphinx.groupFunc = {
        "DAY"		: 0,
        "WEEK"		: 1,
        "MONTH"		: 2,
        "YEAR"		: 3,
        "ATTR"		: 4,
        "ATTRPAIR"	: 5
    };

    // Commands
    Sphinx.command = {
        "SEARCH"  		: 0,
        "EXCERPT" 		: 1,
        "UPDATE"  		: 2,
        "KEYWORDS"		: 3,
        "PERSIST" 		: 4,
        "STATUS"  		: 5,
        "QUERY"   		: 6,
        "FLUSHATTRS"	: 7
    };

    // Current version client commands
    Sphinx.clientCommand = {
        "SEARCH"	: 0x118,
        "EXCERPT"	: 0x103,
        "UPDATE"	: 0x102,
        "KEYWORDS"	: 0x100,
        "STATUS"	: 0x100,
        "QUERY"		: 0x100,
        "FLUSHATTRS": 0x100
    };

    Sphinx.statusCode = {
        "OK":      0,
        "ERROR":   1,
        "RETRY":   2,
        "WARNING": 3
    };

    Sphinx.filterTypes = {
    	"VALUES"		: 0,
    	"RANGE"			: 1,
    	"FLOATRANGE"	: 2
    };

    Sphinx.attribute = {
        "INTEGER":        1,
        "TIMESTAMP":      2,
        "ORDINAL":        3,
        "BOOL":           4,
        "FLOAT":          5,
        "BIGINT":         6,
        "STRING":         7,
        "MULTI":          0x40000000 
    };


    var query_parameters = {
    		offset				: 0,
    		limit				: 20,
    		mode				: Sphinx.searchMode.ALL,
    		weights				: [],
    		sort				: Sphinx.sortMode.RELEVANCE,
    		sortby				: "",
    		min_id				: 0,
    		max_id				: 0,
    		filters				: [],
    		groupby				: "",
    		groupfunc			: Sphinx.groupFunc.DAY,
    		groupsort			: "@group desc",
    		groupdistinct		: "",
    		maxmatches			: 1000,
    		cutoff				: 0,
    		retrycount			: 0,
    		retrydelay			: 0,
    		anchor				: [],
    		indexweights		: [],
    		ranker				: Sphinx.rankingMode.PROXIMITY_BM25,
    		maxquerytime		: 0,
    		weights				: [],
    		overrides 			: [],
    		selectlist			: "*",
            indexes				: '*',
            comment				: '',
        	query				: "",
    		error				: "", // per-reply fields (for single-query case)
    		warning				: "",
    		connerror			: false,

    		reqs				: [],	// requests storage (for multi-query case)
    		mbenc				: "",
    		arrayresult			: true,
    		timeout				: 0
        };

    
    function ClientObj() {
    	var self = this;
    	
    	this._responseProcessor = null;
    	
    	this._server_conn = {};
    	this._path = null;
    	this._port = null;
    	this._host = null;
    	
		this.setServer = function(host, port) {
			if (host) {
				if (host.charAt(0) == '/') {
					self._path = host;
					return;
				} else if (host.substr(0,7) == 'unix://') {
					self._path = host.substr(7);
					return;
				}
			}
			
			self._host = host || _defaults.host;
			self._port = port || _defaults.port;
			self._path = null;
		}


		this.open = function(callback) {
			if ((self._server_conn != undefined) && (self._server_conn.hasOwnProperty('writeable'))) {
				return null;
			}
	
			_connect(self, function() {
				if (!self._server_conn.hasOwnProperty('writable')) return null;
		
				self._responseProcessor = new ResponseObj();

				if (self._server_conn.readyState == 'open') {
		        	self._server_conn.on('data', self._responseProcessor.append);
					var cmd = Buffer.makeWriter();
					cmd.push.int16(Sphinx.command[_defaults.connection_mode]);
					cmd.push.int16(0);
					cmd.push.int32(4);		// have to push a bogus query
					cmd.push.int32(1);
					self._server_conn.write(cmd.toBuffer());
		
			    } else {
			        err = new Error('Connection is ' + self._server_conn.readyState + ' in open');
			        self._server_conn = {};
			        throw err;
			    }
		
				callback(null);
			});
			
	        return true;
		};
		
	    this.query = function(query_raw, callback) {
	    	var query = {};
	    	
	        if (query_raw.query) {
	            for (x in query_parameters) {
	            	if (query_raw.hasOwnProperty(x)) {
	                    query[x] = query_raw[x];            		
	            	} else {
	                    query[x] = query_parameters[x];
	            	}
	            }
	        } else {
	            query = query_raw.toString();
	        }
	        
			var request = Buffer.makeWriter(); 
			request.push.int16(Sphinx.command.SEARCH);
			request.push.int16(Sphinx.clientCommand.SEARCH);
			
	        request.push.int32(0); // This will be request length
	        request.push.int32(0);
	        request.push.int32(1);
	        
			request.push.int32(query.offset);
			
			request.push.int32(query.limit);

			request.push.int32(query.mode);
			request.push.int32(query.ranker);
			
			request.push.int32(query.sort);
			
	        request.push.lstring(query.sortby); 
	        request.push.lstring(query.query); // Query text
	        request.push.int32(query.weights.length); 
	        for (var weight in query.weights) {
	            request.push.int32(parseInt(weight));
	        }

	        request.push.lstring(query_parameters.indexes); // Indexes used

	        request.push.int32(1); // id64 range marker

	        //request.push.int32(0);
	        request.push.int64(0, query.min_id); // This is actually supposed to be two 64-bit numbers
	        //request.push.int32(0);				//  However, there is a caveat about using 64-bit ids
	        request.push.int64(0, query.max_id); 

	        request.push.int32(query.filters.length); 
	        for (var filter in query.filters) {
	            request.push.int32(filter.attr.length);
	            request.push_lstring(filter.attr);
	            request.push.int32(filter.type);
	            switch (filter.type) {
	            	case Sphinx.filterTypes.VALUES:
	            		request.push.int32(filter.values.length);
	            		for (var value in filter.values) {
	                		//request.push.int32(0);		// should be a 64-bit number
	                		request.push.int64(0, value);
	            		}
	            		break;
	            	case Sphinx.filterTypes.RANGE:
	            		//request.push.int32(0);		// should be a 64-bit number
	            		request.push.int64(0, filter.min);
	            		//request.push.int32(0);		// should be a 64-bit number
	            		request.push.int64(0, filter.max);
	            		break;
	            	case Sphinx.filterTypes.FLOATRANGE:
	            		request.push.float(filter.min);
	            		request.push.float(filter.max);
	            		break;
	            }
	        }
	        
	        request.push.int32(query.groupfunc);
	        request.push.lstring(query.groupby); // Groupby length

	        request.push.int32(query.maxmatches); // Maxmatches, default to 1000

	        request.push.lstring(query.groupsort); // Groupsort

	        request.push.int32(query.cutoff); // Cutoff
	        request.push.int32(query.retrycount); // Retrycount
	        request.push.int32(query.retrydelay); // Retrydelay

	        request.push.lstring(query.groupdistinct); // Group distinct

	        if (query.anchor.length == 0) {
	            request.push.int32(0); // no anchor given
	        } else {
	            request.push.int32(1); // anchor point in radians
	            request.push.lstring(query.anchor["attrlat"]); // Group distinct
	            request.push.lstring(query.anchor["attrlong"]); // Group distinct
	    		request.push.float(query.anchor["lat"]);
	    		request.push.float(query.anchor["long"]);
	        }

	        request.push.int32(query.indexweights.length);
	        for (var i in query.indexweights) {
	            request.push.int32(i);
	            request.push.int32(query.indexweights[i]);
	        }

	        request.push.int32(query.maxquerytime); 

	        request.push.int32(query.weights.length);
	        for (var i in query.weights) {
	            request.push.int32(i);
	            request.push.int32(query.weights[i]);
	        }

	        request.push.lstring(query.comment); 

	        request.push.int32(query.overrides.length);
	        for (var i in query.overrides) {
	            request.push.lstring(query.overrides[i].attr); 
	            request.push.int32(query.overrides[i].type);
	            request.push.int32(query.overrides[i].values.length);
	            for (var id in query.overrides[i].values) {
	                request.push.int64(id);
	                switch (query.overrides[i].type) {
		                case Sphinx.attribute.FLOAT:
		                    request.push.float(query.overrides[i].values[id]);
		                    break;
		                case Sphinx.attribute.BIGINT:
		                    request.push.int64(query.overrides[i].values[id]);
		                    break;
		                default:
		                    request.push.int32(query.overrides[i].values[id]);
		                    break;
	                }
	            }
	        }

	        request.push.lstring(query.selectlist); // Select-list

	        var request_buf = request.toBuffer();
	        var req_length = Buffer.makeWriter();
	        req_length.push.int32(request_buf.length - 8);
	        req_length.toBuffer().copy(request_buf, 4, 0);

	        //console.log('Sending request of ' + request_buf.length + ' bytes');
	        
	        self._responseProcessor.callback = callback;
	        self._server_conn.write(request_buf);
	    };

	    this.disconnect = function() {
	    	self._server_conn.end();
	    };
    }

    
	// Connect to Sphinx server, for internal use only (open & query)
    function _connect(client, callback) {
		if ((client._server_conn != undefined) && (client._server_conn.hasOwnProperty('writeable'))) {
			// check to see if we can still read and write to the socket
			if (client._server_conn.readable && client._server_conn.writeable) {
				callback();
				return;
			}

			// zombie, kill it and open a new one
			client._server_conn.destroy();
			client._server_conn = {};
		}

		try {
			if (client._path != null) {
				client._server_conn = new tcp.createConnection(client._path);
			} else {
				client._server_conn = new tcp.createConnection(client._port, client._host);
			}
			
            client._server_conn.on('connect', function() {
            	client._server_conn.on('data', function(data) {
	            	client._server_conn.removeAllListeners('data');
	    			client._server_conn.setNoDelay(true);
	    			client._server_conn.on('error', function(exp) {
	    				console.log('Error: ' + exp);
	    			});
	    			client._server_conn.on('end', function() { });

					var decoder = data.toReader();
	                status  = decoder.int16();
	                version = decoder.int16();
	                len  = decoder.int32();

	                console.log('Sphinx connection to server version ' + version);
	                if (version < 1) {
	                	err = new Error("Invalid Sphinx Server version");
	                	throw(err);
	                }
					var version_number = Buffer.makeWriter();
					version_number.push.int32(1);
	                client._server_conn.write(version_number.toBuffer());
	                callback();
            	});

            });

		}
		catch (e) {
			console.log('Error: ' + sys.inspect(e));
		}
    }


    function ResponseObj() {
    	var self = this;
    	
    	this.status = null;
        this.version = null;
        this.length = 0;
        this.data = new Buffer(0);
        this.callback = null;
        
        this.parseHeader = function() {
            if (self.status === null && self.data.length >= 8) {
                // console.log('Answer length: ' + (this.data.length));
				var decoder = self.data.toReader();

                self.status  = decoder.int16();
                self.version = decoder.int16();
                self.length  = decoder.int32();
                //console.log('Receiving answer with status ' + this.status + ', version ' + this.version + ' and length ' + this.length);

				self.data = self.data.slice(8, self.data.length);
            } else if (self.data.length == 4) {
				var decoder = self.data.toReader();

                self.status  = decoder.int16();
                self.version = decoder.int16();
                console.log('Receiving error with status ' + this.status + ', version ' + this.version + ' and length ' + this.length);
            }
        };
        

        this.append = function(data) {
            var new_buffer = new Buffer(self.data.length + data.length);
            self.data.copy(new_buffer, 0, 0);
            data.copy(new_buffer, self.data.length, 0);
            self.data = new_buffer;
            self.parseHeader();
            self.deliverDataIfDone();
        };
        
        this.done = function() {
            return self.data.length >= self.length;
        };
        
        this.checkResponse = function(search_command) {
            var errmsg = '';
            if (self.length !== self.data.length) {
                errmsg += "Failed to read searchd response (status=" + self.status + ", ver=" + self.version + ", len=" + self.length + ", read=" + self.data.length + ")";
            }

            if (self.version < search_command) {
                errmsg += "Searchd command older than client's version, some options might not work";
            }

            if (self.status == Sphinx.statusCode.WARNING) {
                errmsg += "Server issued WARNING: " + self.data;
            }

            if (self.status == Sphinx.statusCode.ERROR) {
                errmsg += "Server issued ERROR: " + self.data;
            }
            return errmsg;
        };
        
        this.deliverDataIfDone = function() {
            if (self.done()) {
                var answer = "";
                var errmsg = self.checkResponse(Sphinx.clientCommand.SEARCH);
                if (!errmsg) {
                    answer = parseSearchResponse(self.data);
                    self.callback(null, answer);
                } else {
                    self.callback(new Error(errmsg), "{}");
                }
                
            	self.status = null;
                self.version = null;
                self.length = 0;
                self.data = new Buffer(0);
                self.callback = null;
            }
        };
    }

    var parseSearchResponse = function (data) {
        var output = {};
        // var response = new bits.Decoder(data);
        var response = data.toReader();
        var i;

        output.status = response.int32();
	if (output.status != 0) {
		return(response.lstring());
	}
        output.num_fields = response.int32();

        output.fields = [];
        output.attributes = [];
        output.matches = [];

        // Get fields
        for (i = 0; i < output.num_fields; i++) {
            var field = {};

            field.name = response.lstring();

            output.fields.push(field);
        }

        output.num_attrs = response.int32();

        // Get attributes
        for (i = 0; i < output.num_attrs; i++) {
            var attribute = {};

            attribute.name = response.lstring();
            attribute.type = response.int32();
            output.attributes.push(attribute);
        }

        output.match_count = response.int32();
        output.id64 = response.int32();

        // Get matches
        for (i = 0; i < output.match_count; i++) {
            var match = {};

            // Here server tells us which format for document IDs
            // it uses: int64 or int32
            if (output.id64 == 1) {
                // get the 64-bit result, but only use the lower half for now
                var id64 = response.int64();
                match.doc = id64[1];
                match.weight = response.int32();
            } else {
                // Good news: document id fits our integers size :)
                match.doc = response.int32();
                match.weight = response.int32();
            }

            match.attrs = {};

            //
            var attr_value;
            // var attribute;
            for (attribute in output.attributes) {
                // BIGINT size attributes (64 bits)
                if (output.attributes[attribute].type == Sphinx.attribute.BIGINT) {
                    attr_value = response.int32();
                    attr_value = response.int32();
                    match.attrs[output.attributes[attribute].name] = attr_value;
                    continue;
                }

                // FLOAT size attributes (32 bits)
                if (output.attributes[attribute].type == Sphinx.attribute.FLOAT) {
                    attr_value = response.int32();
                    match.attrs[output.attributes[attribute].name] = attr_value;
                    continue;
                }

                // STRING attributes
                if (output.attributes[attribute].type == Sphinx.attribute.STRING) {
                    attr_value = response.lstring();
                    match.attrs[output.attributes[attribute].name] = attr_value;
                    continue;
                }

                // We don't need this branch right now,
                // as it is covered by previous `if`
                // @todo: implement MULTI attribute type
                attr_value = response.int32();
                match.attrs[output.attributes[attribute].name] = attr_value;
            }

            output.matches.push(match);

        }

        output.total = response.int32();
        output.total_found = response.int32();
        output.msecs = response.int32();
        output.words_count = response.int32();
        output.words = new Object();
        for (i = 0; i < output.words_count; i++) {
            var word = response.lstring();
            output.words[word] = new Object();
            output.words[word]["docs"] = response.int32();
            output.words[word]["hits"] = response.int32();
        }
        
        return output;
    };
    
    limestone.SphinxClient = function() {
    	var nouvelle_client = new ClientObj();
        nouvelle_client.setServer(null,null);
        return nouvelle_client;
    };
    
