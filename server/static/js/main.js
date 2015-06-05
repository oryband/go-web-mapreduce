/* jshint browser: true, devel: true, jquery: true, evil: true */
/* global SockJS */

$(document).ready(function() {
    "use strict";

    $("#add-worker").click(function() {
        var sock = new SockJS("/worker");

        // sock.onopen = function() {};
        // sock.onclose = function() {};

        sock.onmessage = function(e) {
            var data = JSON.parse(e.data);

            if (data.meta.type !== "new job") {
                sock.close();
                return;
            }

            var f = eval(data.code),
                response = {
                    "meta": {
                        "type": "job complete",
                        "jobID": data.meta.jobID,
                        "jobType": data.meta.jobType
                    },
                };

            if (data.meta.jobType === "map job") {
                var mapOutput = {};

                $.each(data.input, function(_, input) {
                    var result = f(input);
                    if (!(result[0] in mapOutput)) {
                        mapOutput[result[0]] = [];
                    }
                    mapOutput[result[0]].push(result[1]);
                });

                response[data.meta.jobType + "_output"] = mapOutput;
            } else if (data.meta.jobType === "reduce job") {
                var reduceOutput = [];

                $.each(data.input, function(_, input) {
                    var result = f(input);
                    if (!(result[0] in reduceOutput)) {
                        reduceOutput[result[0]] = [];
                    }
                    reduceOutput.push(result[1]);
                });

                response[data.meta.jobType + "_output"] = reduceOutput;
            }

            console.log(JSON.stringify(response));
            sock.send(JSON.stringify(response));
        };
    });
});
