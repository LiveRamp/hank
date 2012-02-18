<link rel='stylesheet' href='css/global.css' type='text/css'></link>

<!-- Ajax Reloading -->

<script type="text/javascript">

var asyncReloadPeriod = 5000;

var elementUniqClasses = [];

function addAsyncReload(elementUniqClass) {
  elementUniqClasses.push(elementUniqClass);
}

function performAsyncReload() {
  var xmlhttp;
  if (window.XMLHttpRequest) { // code for IE7+, Firefox, Chrome, Opera, Safari
    xmlhttp = new XMLHttpRequest();
  } else { // code for IE6, IE5
    xmlhttp = new ActiveXObject("Microsoft.XMLHTTP");
  }
  xmlhttp.onreadystatechange = function() {
    if (xmlhttp.readyState == 4) {
      var tempDiv = document.createElement('div');
      tempDiv.innerHTML = xmlhttp.responseText;
      for (var i = 0; i < elementUniqClasses.length; i++) {
        if (xmlhttp.status == 200) {
          document.getElementsByClassName(elementUniqClasses[i])[0].innerHTML
          = tempDiv.getElementsByClassName(elementUniqClasses[i])[0].innerHTML;
          document.getElementsByClassName(elementUniqClasses[i])[0].style.opacity
          = tempDiv.getElementsByClassName(elementUniqClasses[i])[0].style.opacity;
        } else {
          document.getElementsByClassName(elementUniqClasses[i])[0].style.opacity = "0.2";
        }
      }
      initAsyncReload();
    }
  }
  xmlhttp.open("GET", window.location, true);
  xmlhttp.send();
}

function initAsyncReload() {
  setTimeout("performAsyncReload()", asyncReloadPeriod);
}

window.onload = initAsyncReload();

</script>

<!-- Tooltip. Script source: http://sixrevisions.com/tutorials/javascript_tutorial/create_lightweight_javascript_tooltip/ -->

<script type="text/javascript">
var tooltip=function(){
 var id = 'tt';
 var top = 3;
 var left = 3;
 var maxw = 2000;
 var speed = 10;
 var timer = 5;
 var endalpha = 95;
 var alpha = 0;
 var tt,t,c,b,h;
 var ie = document.all ? true : false;
 return{
  show:function(contentId, title, w){
   if(tt == null){
    tt = document.createElement('div');
    tt.setAttribute('id',id);
    t = document.createElement('div');
    t.setAttribute('id',id + 'top');
    c = document.createElement('div');
    c.setAttribute('id',id + 'cont');
    b = document.createElement('div');
    b.setAttribute('id',id + 'bot');
    tt.appendChild(t);
    tt.appendChild(c);
    tt.appendChild(b);
    document.body.appendChild(tt);
    document.onmousemove = this.pos;
   }
   tt.style.display = 'block';
   t.innerHTML = title;
   c.innerHTML = document.getElementById(contentId).innerHTML;
   tt.style.width = w ? w + 'px' : 'auto';
   if(!w && ie){
    t.style.display = 'none';
    b.style.display = 'none';
    tt.style.width = tt.offsetWidth;
    t.style.display = 'block';
    b.style.display = 'block';
   }
  if(tt.offsetWidth > maxw){tt.style.width = maxw + 'px'}
  h = parseInt(tt.offsetHeight) + top;
  clearInterval(tt.timer);
  tt.timer = setInterval(function(){tooltip.fade(1)},timer);
  },
  pos:function(e){
   var u = ie ? event.clientY + document.documentElement.scrollTop : e.pageY;
   var l = ie ? event.clientX + document.documentElement.scrollLeft : e.pageX;
   var pos_top = (u - h);
   if (pos_top < 0) {
     pos_top = 0;
   }
   var pos_left = (l + left);
   tt.style.top = pos_top + 'px';
   tt.style.left = pos_left + 'px';
  },
  fade:function(d){
   var a = alpha;
   if((a != endalpha && d == 1) || (a != 0 && d == -1)){
    var i = speed;
   if(endalpha - a < speed && d == 1){
    i = endalpha - a;
   }else if(alpha < speed && d == -1){
     i = a;
   }
   alpha = a + (i * d);
   tt.style.opacity = alpha * .01;
   tt.style.filter = 'alpha(opacity=' + alpha + ')';
  }else{
    clearInterval(tt.timer);
     if(d == -1){tt.style.display = 'none'}
  }
 },
 hide:function(){
  clearInterval(tt.timer);
   tt.timer = setInterval(function(){tooltip.fade(-1)},timer);
  }
 };
}();
</script>
