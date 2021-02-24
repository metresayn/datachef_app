from app import app
from flask import render_template

@app.route("/")
def index():
    return render_template("index.html",image_name = 'img/Test_img.jpg')


@app.route("/about")
def about():
	return "<h1 style='color: red;'>I'm a red H1 heading!</h1>"