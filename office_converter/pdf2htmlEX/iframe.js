var console = {
    panel: $(parent.document.body).append('<div>'),
    log: function(m){
        this.panel.prepend('<div>'+m+'</div>');
    }       
};

$(window).on('message', function(event) {
    var e = event.originalEvent;
    if (e.data && e.data.scale) {
        var scale = e.data.scale;
        // console.log('scale to ' + scale);
        $('.pf').css({
            'transform':'scale(' + scale + ')',
            '-webkit-transform':'scale(' + scale + ')',
            '-ms-transform':'scale(' + scale + ')',
            'transform-origin':'top left',
            '-webkit-transform-origin':'top left',
            '-ms-transform-origin':'top left'
        });
        $('#page-container').css({'overflow':'hidden', 'background':'none'});
    }
});
