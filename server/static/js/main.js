/* jshint browser: true, devel: true, jquery: true, maxlen: 120, evil: true */
/* global SockJS */

$(document).ready(function() {
    "use strict";

    var map = function(f, input) {
        var out = {};

        $.each(input, function(_, input) {
            var result = f(input);
            var pi = result[0], // pi means partition index.
                o = result[1];

            if (!(pi in out)) {
                out[pi] = [];
            }

            out[pi].push(o);
        });

        return out;
    },
    reduce = function(f, input) {
        var out = [];
        $.each(input, function(_, input) {
            out.push(f(input));
        });
        return out;
    };

    $("#add-worker").click(function() {
        var sock = new SockJS("/worker");

        // sock.onopen = function() {};
        // sock.onclose = function() {};

        sock.onmessage = function(e) {
            console.log("new message received");

            var data = JSON.parse(e.data);
            if (data.meta.type !== "new job") {
                console.error("unknown message type " + data.meta.type);
                sock.close();
                return;
            }

            var f = eval(data.code),
                response = {
                    "meta": {
                        "type": "job complete",
                        "jobID": data.meta.jobID,
                        "jobType": data.meta.jobType,
                    },
                };

            console.log("input: " + data.input);

            switch (data.meta.jobType) {
                case "map":
                    console.log("map job received");
                    response[data.meta.jobType + "_output"] = map(f, data.input);
                    break;
                case "reduce":
                    console.log("reduce job received");
                    response[data.meta.jobType + "_output"] = reduce(f, data.input);
                    break;
                default:
                    console.error("unknown job type " + data.meta.jobType);
            }

            var resStr = JSON.stringify(response);

            console.log("response: " + resStr);

            sock.send(resStr);
        };
    });
});
