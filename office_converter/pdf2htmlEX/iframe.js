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

  $(function() {
    $('image.bi').each(function(index, image) {
      var image = $(image);

      // Newer version of pdf2htmlEX use a different way to render the images
      // embedded in the pdf.
      //
      // See the source code for the difference:
      //
      // old: https://github.com/coolwanglu/pdf2htmlEX/blob/v0.9/src/HTMLRenderer/general.cc#L218-L219
      // new: https://github.com/coolwanglu/pdf2htmlEX/blob/v0.14.6/src/BackgroundRenderer/SplashBackgroundRenderer.cc#L168-L173
      if (image.hasClass('x0')) {
        // If the image has class 'x0', then it's the newer version of
        // pdf2htmlEX. We'll update the style to match it.
        image.css({
          'position': 'absolute',
          'border': '0',
          'margin': '0',
          '-ms-user-select': 'none',
          '-moz-user-select': 'none',
          '-webkit-user-select': 'none',
          'user-select': 'none',
        });
      }
    });
  });

};
