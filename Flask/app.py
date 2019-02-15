from flask import Flask, render_template, request
from flask import jsonify
from helper import process_query_get_results
app = Flask(__name__)


@app.route('/', methods= ["GET", "POST"])
def homepage():
    if request.method == 'POST':
        description = request.json['description']
        code = request.json['code']
        tags = request.json['tags']
        if description:
            match_results = process_query_get_results(description, tags, code)
            return match_results
        else:
            return jsonify(result='Input needed')

    return render_template('index.html')


if __name__ == '__main__':
   app.run(debug = True)


