from flask import Flask, render_template, request
from flask import jsonify
from helper import process_query_get_results
import json
app = Flask(__name__)


@app.route('/', methods= ["GET", "POST"])
def homepage():
    if request.method == 'POST':
        title = request.form.get('data[description]')
        tags = request.form.get('data[tags]')
        if title:
            # match_results =[]
            match_results = process_query_get_results(title, tags)
            match_results_json = json.dumps(match_results)
            return match_results_json
        else:
            return jsonify(result='Input needed')

    return render_template('index.html')


if __name__ == '__main__':
   app.run(debug = True)


