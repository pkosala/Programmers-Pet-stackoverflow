$(function() {
              $('a#process_input').bind('click', function() {

              if ($('#description_input').val() == '' || $('#tags_input').val() == '') {
                        var errorMessage = document.getElementById('errorname');
                        errorMessage.innerText = 'Please enter both description and tags to search';
                 } else {
                     var errorMessage = document.getElementById('errorname');
                     errorMessage.innerText = "";
                     $.post('/', {
                          data: {description: $('textarea[name="description"]').val(), tags: $('textarea[name="tags"]').val()},

                }, function(data) {
                        var errorMessage = document.getElementById('errorname');
                        errorMessage.innerText = "";
                        var mydiv = document.getElementById("results");
                        while (mydiv.hasChildNodes()) {
                            mydiv.removeChild(mydiv.lastChild);
                        }
                        match_results = JSON.parse(data);
                        match_results.forEach(function (eachPost) {
                            var id = eachPost['id'];
                            var title = eachPost['title'];
                            var disp_order = eachPost['disp_order'];
                            console.log(eachPost);
                            var aTag = document.createElement('a');
                                        aTag.setAttribute('href',"https://stackoverflow.com/questions/"+id.toString());
                                        aTag.setAttribute('target',"_blank");
                                        aTag.innerHTML = title;
                                        mydiv.appendChild(aTag);

                                        linebreak = document.createElement("br");
                                        mydiv.appendChild(linebreak);
                        });

//                  $('#result').text(data.result);
                });
                 }

                return false;
              });
            });