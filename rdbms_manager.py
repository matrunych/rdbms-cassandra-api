import psycopg2


class RDBMS:
    def __init__(self):
            self.connection = psycopg2.connect(database='books')
            self.cursor = self.connection.cursor()

    def reviews_for_product_id(self, id):
        self.cursor.execute("SELECT * FROM review WHERE product_id = %s;", (id,))
        return self.cursor.fetchall()

    def reviews_for_product_id_with_star_rating(self, id, star_rating):
        self.cursor.execute("SELECT * FROM review WHERE product_id = %s AND star_rating = %s;", (id, star_rating,))
        return self.cursor.fetchall()

    def reviews_for_customer_id(self, id):
        self.cursor.execute("SELECT * FROM review WHERE customer_id = %s;", (id,))
        return self.cursor.fetchall()

    def most_reviewed_items(self, start_date, end_date, n):
        self.cursor.execute("SELECT product_id FROM review \
                            INNER JOIN product on review.product_id=product.id \
                            WHERE  review_date >= %s AND review_date <= %s \
                            GROUP BY product_id ORDER BY COUNT(*) DESC LIMIT %s ;", (start_date, end_date, n,))
        return self.cursor.fetchall()

    def most_productive_customers(self, start_date, end_date, n):
        self.cursor.execute("SELECT customer_id FROM review \
                                    WHERE  review_date >= %s AND review_date <= %s AND verified_purchase = true\
                                    GROUP BY customer_id ORDER BY COUNT(*) DESC LIMIT %s ;", (start_date, end_date, n,))
        return self.cursor.fetchall()

    def most_productive_haters(self, start_date, end_date, n):
        self.cursor.execute("SELECT customer_id FROM review \
                                    WHERE  review_date >= %s AND review_date <= %s AND star_rating < 3\
                                    GROUP BY customer_id ORDER BY COUNT(*) DESC LIMIT %s ;", (start_date, end_date, n,))
        return self.cursor.fetchall()

    def most_productive_backers(self, start_date, end_date, n):
        self.cursor.execute("SELECT customer_id FROM review \
                                    WHERE  review_date >= %s AND review_date <= %s AND star_rating > 3\
                                    GROUP BY customer_id ORDER BY COUNT(*) DESC LIMIT %s ;", (start_date, end_date, n,))
        return self.cursor.fetchall()
