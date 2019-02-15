$(".overlay").hide();
$(function () {
	$('a#process_input').bind('click', function () {

		if ($('#description_input').val() == '' || $('#tags_input').val() == '') {
			var errorMessage = document.getElementById('errorname');
			errorMessage.innerText = 'Please enter both description and tags';
		} else {
			var errorMessage = document.getElementById('errorname');
			errorMessage.innerText = "";
			$.ajax({
				type: "POST",
				url: "/",
				contentType: 'application/json;charset=UTF-8',
				data: JSON.stringify({
					description: $('textarea[name="description"]').val(),
					tags: $('textarea[name="tags"]').val(),
					code: $('textarea[name="code"]').val()
				}),
				beforeSend: function () {
					$(".overlay").show();
				},
				success: function (response) {
					$(".overlay").hide();
					var errorMessage = document.getElementById('errorname');
					errorMessage.innerText = "";
					var query_div = document.getElementById("results_by_query");
					while (query_div.hasChildNodes()) {
						query_div.removeChild(query_div.lastChild);
					}
					var code_div = document.getElementById("results_by_code");
					while (code_div.hasChildNodes()) {
						code_div.removeChild(code_div.lastChild);
					}

					match_results = JSON.parse(response);
					matches_by_query = match_results['matches_by_query']
					matches_by_code = match_results['matches_by_code']
					if (matches_by_query === matches_by_query || array.length == 0) {
						var para = document.createElement("p");
						var node = document.createTextNode("Sorry, did not find any match");
						query_div.appendChild(node);
					} else {
						matches_by_query.forEach(function (eachPost) {
							var id = eachPost['id'];
							var title = eachPost['title'];
							var disp_order = eachPost['disp_order'];
							console.log(eachPost);
							var aTag = document.createElement('a');
							aTag.setAttribute('href', "https://stackoverflow.com/questions/" + id.toString());
							aTag.setAttribute('target', "_blank");
							aTag.innerHTML = title;
							query_div.appendChild(aTag);

							linebreak = document.createElement("br");
							query_div.appendChild(linebreak);
						});
					}
					if (matches_by_code === undefined || matches_by_code.length == 0) {
						var para = document.createElement("p");
						var node = document.createTextNode("Sorry, did not find any match");
						code_div.appendChild(node);
					} else {
						matches_by_code.forEach(function (eachPost) {
							var id = eachPost['id'];
							var title = eachPost['title'];
							var disp_order = eachPost['disp_order'];
							console.log(eachPost);
							var aTag = document.createElement('a');
							aTag.setAttribute('href', "https://stackoverflow.com/questions/" + id.toString());
							aTag.setAttribute('target', "_blank");
							aTag.innerHTML = title;
							code_div.appendChild(aTag);

							linebreak = document.createElement("br");
							code_div.appendChild(linebreak);
						});
					}

				},
				error: function (errorMessage) {
					alert("An error occurred while processing: " + errorMessage);
					$(".overlay").hide();
				}
			});
		}
		return false;
	});
});