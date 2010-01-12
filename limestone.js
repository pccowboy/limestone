var bits = require('./bits');
var tcp = require('tcp');
var sys = require('sys');

var Sphinx = {
    'port':9312
};

(function() {
    // var Sphinx.port = 9312;

    Sphinx.queries = [];

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
        "PROXIMITY_BM25": 0,    ///< default mode, phrase proximity major factor and BM25 minor one
        "BM25": 1,    ///< statistical mode, BM25 ranking only (faster but worse quality)
        "NONE": 2,    ///< no ranking, all matches get a weight of 1
        "WORDCOUNT":3,    ///< simple word-count weighting, rank is a weighted sum of per-field keyword occurence counts
        "PROXIMITY":4,
        "MATCHANY" :5,
        "FIELDMASK":6
    };

    Sphinx.sortMode = {
        "RELEVANCE": 0,
        "ATTR_DESC": 1,
        "ATTR_ASC": 2,
        "TIME_SEGMENTS": 3,
        "EXTENDED": 4,
        "EXPR": 5
    };

    Sphinx.groupMode = {
        "DAY": 0,
        "WEEK": 1,
        "MONTH": 2,
        "YEAR": 3,
        "ATTR": 4,
        "ATTRPAIR": 5
    };

    // Commands
    Sphinx.command = {
        "SEARCH"  : 0,
        "EXCERPT" : 1,
        "UPDATE"  : 2,
        "KEYWORDS": 3,
        "PERSIST" : 4,
        "STATUS"  : 5,
        "QUERY"   : 6
    };

    // Current version client commands
    Sphinx.clientCommand = {
        "SEARCH": 278,
        "EXCERPT": 0x100,
        "UPDATE": 0x102,
        "KEYWORDS": 0x100,
        "STATUS": 0x100,
        "QUERY": 0x100
    }

    Sphinx.statusCode = {
        "OK":      0,
        "ERROR":   1,
        "RETRY":   2,
        "WARNING": 3
    }


    sys.puts('Connecting to searchd...');

    var server_conn = tcp.createConnection(Sphinx.port);

    // disable Nagle algorithm
    server_conn.setNoDelay(true);
    server_conn.setEncoding('binary');

    server_conn.addListener('connect', function () {
        // Sending protocol version
        sys.puts('Sending version number...');
        // Here we must send 4 bytes, '0x00000001'
        server_conn.send((new bits.Encoder()).push_int32(1).toRawString(), 'binary');

        // Waiting for answer
        server_conn.addListener('receive', function(data) {
            // var data_unpacked = binary.unpack('N*', data);
            var receive_listeners = server_conn.listeners('receive');
            var i;
            for (i = 0; i < receive_listeners.length; i++) {
                server_conn.removeListener('receive', receive_listeners[i]);
            }
            var protocol_version = (new bits.Decoder(data)).shift_int32();
            var data_unpacked = {'': 1};

            var composeQuery = function(query) {
                // Header


                var request = (new bits.Encoder(0, Sphinx.clientCommand.SEARCH)).push_int32(0).push_int32(20).push_int32(Sphinx.searchMode.ALL).push_int32(Sphinx.rankingMode.BM25).push_int32(Sphinx.sortMode.RELEVANCE);

                request.push_int32(0); // "sort by" is not supported yet

                request.push_int32(query.length); // Query text length

                request.push_raw_string(query); // Query text

                request.push_int32(0); // weights is not supported yet

                request.push_int32(1).push_raw_string('*'); // Indices used

                request.push_int32(1); // id64 range marker

                request.push_int32(0).push_int32(0).push_int32(0).push_int32(0); // No limits for range

                request.push_int32(0);
                // var req_filters = binary.pack("N", 0); // filters is not supported yet
                request.push_int32(Sphinx.groupMode.DAY);
                request.push_int32(0); // Groupby length
                // var req_grouping = binary.pack("NN", Sphinx.groupMode.DAY, 0); // Basic grouping is supported

                request.push_int32(1000); // Maxmatches, default to 1000

                request.push_int32("@group desc".length); // Groupsort
                request.push_raw_string("@group desc");

                request.push_int32(0); // Cutoff
                request.push_int32(0); // Retrycount
                request.push_int32(0); // Retrydelay

                request.push_int32(0); // Group distinct

                request.push_int32(0); // anchor is not supported yet

                request.push_int32(0); // Per-index weights is not supported yet

                request.push_int32(0); // Max query time is set to 0

                request.push_int32(0); // Per-field weights is not supported yet

                request.push_int32(0); // Comments is not supported yet

                request.push_int32(0); // Atribute overrides is not supported yet

                request.push_int32(1).push_raw_string('*'); // Select-list

                server_conn.send(request.toString(), 'binary');

                sys.puts('Request sent: [' +  request.toString().length + ']');
                var x;
                for (x = 0; x < request.toString().length; x++) {
                    sys.puts(x + ':' + request.toString().charCodeAt(x).toString(16));
                }

                server_conn.addListener('receive', function(data) {
                    // Got response!
                    sys.puts('Answer received:' + data + '[' + data.length + ']');
                    // Command must match the one used in query
                    var response = parseResult(data, Sphinx.clientCommand.SEARCH);
                    sys.puts('Answer data:' + JSON.stringify(response));
                });
            };

            var parseResult = function(data, search_command) {
                var output = {};
                var response = new bits.Decoder(data);
                var position = 0;
                var data_length = data.length;

                output.status = response.shift_int16();
                output.version = response.shift_int16();

                output.length = response.shift_int32();

                if (output.length != data.length - 8) {
                    sys.puts("failed to read searchd response (status=" + output.status + ", ver=" + output.version + ", len=" + output.length + ", read=" + (data.length - 8) + ")");
                }

                if (output.version < search_command) {
                    sys.puts("searchd command older than client's version, some options might not work");
                }

                if (output.status == Sphinx.statusCode.WARNING) {
                    sys.puts("WARNING: ");
                }

                return output;
            }

            sys.puts('Server data received: ' + protocol_version);
            if (data_unpacked[""] >= 1) {

                // Remove listener after handshaking
                for (listener in server_conn.listeners('receive')) {
                    server_conn.removeListener('receive', listener);
                }

                server_conn.removeListener('receive');
                // Here is our answer. It contains 1+
                sys.puts('Connection established, sending query');
                sys.puts('text'.length);

                composeQuery('test');

                //server_conn.close();
            }
        });
    });
})();