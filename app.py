from rdbms_manager import RDBMS
from flask import Flask, jsonify
from cassandra_manager import Cassandra

app = Flask(__name__)


@app.route('/reviews_for_product_id/<product_id>', methods=["GET"])
def reviews_for_product_id(product_id):
    res = jsonify(db.reviews_for_product_id(product_id))
    res.status_code = 200
    return res


@app.route('/reviews_for_product_id_with_star_rating/<string:product_id>/<int:star_rating>', methods=["GET"])
def reviews_for_product_id_with_star_rating(product_id, star_rating):
    res = jsonify(db.reviews_for_product_id_with_star_rating(product_id, star_rating))
    res.status_code = 200
    return res


@app.route('/reviews_for_customer_id/<string:customer_id>', methods=["GET"])
def reviews_for_customer_id(customer_id):
    res = jsonify(db.reviews_for_customer_id(customer_id))
    res.status_code = 200
    return res


@app.route('/most_reviewed_items/<string:start_date>/<string:end_date>/<int:n>', methods=["GET"])
def most_reviewed_items(start_date, end_date, n):
    res = jsonify(db.most_reviewed_items(start_date, end_date, n))
    res.status_code = 200
    return res


@app.route('/most_productive_customers/<string:start_date>/<string:end_date>/<int:n>', methods=["GET"])
def most_productive_customers(start_date, end_date, n):
    res = jsonify(db.most_productive_customers(start_date, end_date, n))
    res.status_code = 200
    return res


@app.route('/most_productive_haters/<string:start_date>/<string:end_date>/<int:n>', methods=["GET"])
def most_productive_haters(start_date, end_date, n):
    res = jsonify(db.most_productive_haters(start_date, end_date, n))
    res.status_code = 200
    return res


@app.route('/most_productive_backers/<string:start_date>/<string:end_date>/<int:n>', methods=["GET"])
def most_productive_backers(start_date, end_date, n):
    res = jsonify(db.most_productive_backers(start_date, end_date, n))
    res.status_code = 200
    return res


if __name__ == '__main__':
    # db = RDBMS()

    db = Cassandra(user='cassandra', password='password', ip_address='ip_address')

    app.run(debug=False)
