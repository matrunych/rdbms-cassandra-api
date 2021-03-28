from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd

if __name__ == '__main__':
    filepath = 'file.tsv'

    auth_provider = PlainTextAuthProvider('cassandra', 'password')
    df = pd.read_csv(filepath, sep='\t', header=0)
    cluster = Cluster(['ip_address'], auth_provider=auth_provider)
    session = cluster.connect("review_data")

    prepared_q1 = session.prepare("""
                                INSERT INTO review_data.review_by_product (marketplace,	customer_id, review_id,
                                product_id, product_parent,	product_title, product_category, star_rating, helpful_votes,
                                total_votes, vine,	verified_purchase, review_headline, review_body)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?)
                                """)

    prepared_q3 = session.prepare("""
                                    INSERT INTO review_data.review_by_customer (marketplace, customer_id, review_id,
                                product_id, product_parent,	product_title, product_category, star_rating, helpful_votes,
                                total_votes, vine,	verified_purchase,	review_headline, review_body)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                   """)


    for index, row in df.iterrows():
        marketplace = row[0]
        customer_id = str(row[1])
        review_id = row[2]
        product_id = row[3]
        product_parent = str(row[4])
        product_title = row[5]
        product_category = row[6]
        star_rating = row[7]
        helpful_votes = row[8]
        total_votes = row[9]
        vine = row[10]
        verified_purchase = row[11]
        review_headline = row[12]
        review_body = row[13]
        review_date = row[14]

        session.execute(prepared_q1, [marketplace,	customer_id, review_id,	product_id, product_parent,	product_title,
                                       product_category, star_rating, helpful_votes, total_votes, vine, verified_purchase,
                                       review_headline, review_body])
        session.execute(prepared_q3, [marketplace, customer_id, review_id, product_id, product_parent, product_title,
                                      product_category, star_rating, helpful_votes, total_votes, vine,
                                      verified_purchase,
                                      review_headline,
                                      review_body])
