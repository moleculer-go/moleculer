window.changeVersion = function(select) {
  var ver = select.value;
  console.log("Change to " + '/' + ver + '/');
  window.location.href = '/' + ver + '/';
}
