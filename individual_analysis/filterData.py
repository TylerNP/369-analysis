import csv
import time
import os
import io
import sys

review_path = f'{os.path.dirname(os.getcwd())}/data/steam/review.csv'
english_path = f'{os.path.dirname(os.getcwd())}/data/steam/english_review.csv'

review_header = [
    'app_id', 
    'user_id', 
    'total_playtime', 
    'last_2_week_playtime', 
    'playtime_at_review',
    'last_played', 
    'language', 
    'text', 
    'voted_up',
    'votes_up', 
    'votes_funny', 
    'weighted_vote_score',
    'comment_count', 
    'steam_purchase_status',
    'write_during_early_access'
]
english_header = [
    'app_id', 
    'user_id', 
    'total_playtime', 
    'last_2_week_playtime', 
    'playtime_at_review',
    'last_played', 
    'text', 
    'voted_up',
    'votes_up', 
    'votes_funny', 
    'weighted_vote_score',
    'comment_count', 
    'steam_purchase_status',
    'write_during_early_access'
]

start_time = time.perf_counter_ns()

# Increase field size since the review text is stored as a field
csv.field_size_limit(sys.maxsize)

with open(review_path, "rb") as text_file:
    with io.TextIOWrapper(text_file, encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        header_indices = {header: index for index, header in enumerate(review_header)}
        with open(english_path, 'w', encoding='utf-8') as english_file:

            english_writer = csv.DictWriter(english_file, fieldnames=english_header)
            english_writer.writeheader()

            reviews = []

            for i, row in enumerate(csv_reader):
                # Test segment
                # if i > 10:
                #     break
                if row[header_indices['language']] == 'english':
                    review = {
                        'app_id': row[header_indices['app_id']],
                        'user_id': row[header_indices['user_id']],
                        'total_playtime': row[header_indices['total_playtime']],
                        'last_2_week_playtime': row[header_indices['last_2_week_playtime']],
                        'playtime_at_review': row[header_indices['playtime_at_review']],
                        'last_played': row[header_indices['last_played']],
                        'text': row[header_indices['text']],
                        'voted_up': row[header_indices['voted_up']],
                        'votes_up': row[header_indices['votes_up']],
                        'votes_funny': row[header_indices['votes_funny']],
                        'weighted_vote_score': row[header_indices['weighted_vote_score']],
                        'comment_count': row[header_indices['comment_count']],
                        'steam_purchase_status': row[header_indices['steam_purchase_status']],
                        'write_during_early_access': row[header_indices['write_during_early_access']]
                    }
                    reviews.append(review)
                
                if (len(reviews) % 100000 == 0):
                    english_writer.writerows(reviews)
                    reviews = []
            if (len(reviews)):
                english_writer.writerows(reviews)

end_time = time.perf_counter_ns()
print(f"Took {(end_time-start_time)/1000000} ms")