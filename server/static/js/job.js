window.onload = function() {
    'use strict';

    function map() {
        // count the number of words in the body of document
        var words = document.body.innerHTML.split(/\n|\s/).length;
        emit('reduce', {'count': words});
    }

    function reduce() {
        // sum up all the word counts
        var sum = 0;
        var docs = document.body.innerHTML.split(/\n/);
        for each (num in docs) { sum+= parseInt(num) > 0 ? parseInt(num) : 0 }
        emit('finalize', {'sum': sum});
    }

    function emit(phase, data) {
    }
};
