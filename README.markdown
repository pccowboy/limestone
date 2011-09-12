# Limestone is a Sphinx search server connector for Node.js #

Usage:
```Javascript
    var limestone = require("limestone/limestone"),
        sys = require("sys");

	client = limestone.SphinxClient();
	
	// default is to open localhost:9312
    client.open(function(err) {
        if (err) {
            sys.puts('Connection error: ' + err);
        }
        sys.puts('Connected, sending query');
        client.query({'query':'test', maxmatches:1}, function(err, answer) {
            client.disconnect();
            sys.puts("Extended search for 'test' yielded " + answer.match_count + " results: " + JSON.stringify(answer));
        });
    });
```
## INSTALL ##
```bash
$ npm install limestone
```
## Other interesting methods ##

		client.setServer(host, port)
		
	Sets the server that a client points to.  After setting the server, you must open() a connection to it.
	
## CAVEATS ##

Very little of the Sphinx client functionality has been tested.  Almost none, in fact, for my part.

## Changelog ##
0.1.2	
	* Refactor response and client to use persistent connections
	* Refactor client to make it a poolable object

## LICENSE ##

 This module is released under the [MIT License] [license].
 
 [homepage]: http://github.com/kurokikaze/limestone/
 [license]: http://www.opensource.org/licenses/mit-license.php
