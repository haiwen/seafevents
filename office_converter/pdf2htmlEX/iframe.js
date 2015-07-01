var jquery_file = document.getElementById('jquery');
jquery_file.src = window.parent.document.body.getElementsByTagName('script')[0].src;
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
