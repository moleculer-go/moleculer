(function(){
  'use strict';

  var body = document.getElementsByTagName('body')[0];
  var navToggle = document.getElementById('mobile-nav-toggle');
  var container = document.getElementById('container');
  var dimmer = document.getElementById('mobile-nav-dimmer');
  var CLASS_NAME = 'mobile-nav-on';
  if (!navToggle) return;

  navToggle.addEventListener('click', function(e){
    e.preventDefault();
    e.stopPropagation();
    body.classList.toggle(CLASS_NAME);
  });

  dimmer.addEventListener('click', function(e){
    if (!body.classList.contains(CLASS_NAME)) return;

    e.preventDefault();
    body.classList.remove(CLASS_NAME);
  });

  // Toggle sidebar on swipe
  var start = {}, end = {}

  document.body.addEventListener('touchstart', function (e) {
    start.x = e.changedTouches[0].clientX
    start.y = e.changedTouches[0].clientY
  })

  document.body.addEventListener('touchend', function (e) {
    end.y = e.changedTouches[0].clientY
    end.x = e.changedTouches[0].clientX

    var xDiff = end.x - start.x
    var yDiff = end.y - start.y

    if (Math.abs(xDiff) > Math.abs(yDiff)) {
      if (xDiff > 0 && start.x <= 80) 
        body.classList.toggle(CLASS_NAME);
      else 
        body.classList.remove(CLASS_NAME);
    }
  })


})();