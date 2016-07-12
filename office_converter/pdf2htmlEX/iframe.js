var jquery_file = document.getElementById('jquery');

// Load the jquery src from the parent document. Mainly because we can't use
// MEDIA_URL here.
var jq_script_element = window.parent.document.getElementById('jquery');
if (jq_script_element) {
  // 5.2+
  jquery_file.src = jq_script_element.src;
} else {
  // Before 5.2
  jquery_file.src = window.parent.document.body.getElementsByTagName('script')[0].src;
}
jquery_file.onload = function () {
    $(window).on('message', function(event) {
        var e = event.originalEvent;
        if (e.data && e.data.scale) {
            var scale = e.data.scale;
            $('body').css({
                'transform':'scale(' + scale + ')',
                '-webkit-transform':'scale(' + scale + ')',
                '-ms-transform':'scale(' + scale + ')',
                'transform-origin':'top left',
                '-webkit-transform-origin':'top left',
                '-ms-transform-origin':'top left'
            });
        }
    });
};
