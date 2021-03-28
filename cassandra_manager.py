from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class Cassandra:
    def __init__(self, user, password, ip_address):
        auth_provider = PlainTextAuthProvider(user, password)
        self.cluster = Cluster([ip_address], auth_provider=auth_provider)
        self.session = self.cluster.connect("review_data")

    def reviews_for_product_id(self, id):
        res = self.session.execute("SELECT * FROM review_data.review_by_product WHERE product_id = %s;", (id,))
        rows = [r for r in res]
        return rows

    def reviews_for_product_id_with_star_rating(self, id, star_rating):
        res = self.session.execute(
            "SELECT * FROM review_data.review_by_product WHERE product_id = %s AND star_rating = %s;",
            (id, star_rating,))
        rows = [r for r in res]
        return rows

    def reviews_for_customer_id(self, id):
        res = self.session.execute(
            "SELECT * FROM review_data.review_by_customer WHERE customer_id = %s;", (id,))
        rows = [r for r in res]
        return rows
