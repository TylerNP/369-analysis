import os
import csv
import time
import sys
import zipfile
import io

path = f'{os.path.dirname(os.getcwd())}/data/archive.zip'
zip_path = 'all_reviews/all_reviews.csv'
game_path = f'{os.path.dirname(os.getcwd())}/data/steam/game.csv'
user_path = f'{os.path.dirname(os.getcwd())}/data/steam/user.csv'
review_path = f'{os.path.dirname(os.getcwd())}/data/steam/review.csv'

batch_size = 10000

start_time = time.perf_counter_ns()

print(path)

if not os.path.exists(path):
    print('file not found')
    sys.exit(1)

# Increase field size since the review text is stored as a field
csv.field_size_limit(sys.maxsize)

# unzip file for streaming
with zipfile.ZipFile(path, 'r') as z:
    with z.open(zip_path, 'r') as file:
        # decode into utf-8 to read line by line
        with io.TextIOWrapper(file, encoding='utf-8') as text_file:
            # open all files to write
            with open(game_path, 'w', encoding='utf-8') as game_file, \
                 open(user_path, 'w', encoding='utf-8') as user_file, \
                 open(review_path, 'w', encoding='utf-8') as review_file:

                # CSV reading
                csv_reader = csv.reader(text_file)
                headers = next(csv_reader)
                header_indices = {header: index for index, header in enumerate(headers)}
                #print(header_indices)

                # headers for output
                game_header = ['app_id', 'name']
                user_header = ['user_id', 'num_games', 'num_reviews']
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
                
                # file writers
                game_writer = csv.DictWriter(game_file, fieldnames=game_header)
                game_writer.writeheader()
                user_writer = csv.DictWriter(user_file, fieldnames=user_header)
                user_writer.writeheader()
                review_writer = csv.DictWriter(review_file, fieldnames=review_header)
                review_writer.writeheader()

                # Chunk data
                game_uniq = set()
                game_data = []
                user_uniq = set()
                user_data = []
                review_data = []

                # Read file info
                for i, row in enumerate(csv_reader):

                    # Process batch
                    if (i % batch_size == 0 and i != 0):
                        # write here
                        game_writer.writerows(game_data)
                        user_writer.writerows(user_data)
                        review_writer.writerows(review_data)
                        game_data = []
                        user_data = []
                        review_data = []

                    # add game data
                    if row[header_indices['appid']] not in game_uniq:
                        game_uniq.add(row[header_indices['appid']])
                        game = {
                            'app_id': row[header_indices['appid']],
                            'name': row[header_indices['game']],
                        }
                        game_data.append(game)

                    # add user data
                    if row[header_indices['author_steamid']] not in user_uniq:
                        user_uniq.add(row[header_indices['author_steamid']])
                        user = {
                            'user_id': row[header_indices['author_steamid']],
                            'num_reviews': row[header_indices['author_num_reviews']],
                            'num_games': row[header_indices['author_num_games_owned']],
                        }
                        user_data.append(user)

                    # add review data
                    review = {
                        'app_id': row[header_indices['appid']],
                        'user_id': row[header_indices['author_steamid']],
                        'total_playtime': row[header_indices['author_playtime_forever']],
                        'last_2_week_playtime': row[header_indices['author_playtime_last_two_weeks']],
                        'playtime_at_review': row[header_indices['author_playtime_at_review']],
                        'last_played': row[header_indices['author_last_played']],
                        'language': row[header_indices['language']],
                        'text': row[header_indices['review']],
                        'voted_up': row[header_indices['voted_up']],
                        'votes_up': row[header_indices['votes_up']],
                        'votes_funny': row[header_indices['votes_funny']],
                        'weighted_vote_score': row[header_indices['weighted_vote_score']],
                        'comment_count': row[header_indices['comment_count']],
                        'steam_purchase_status': 2*int(row[header_indices['steam_purchase']]) + int(row[header_indices['received_for_free']]), # Use int to determine purchase status 
                        'write_during_early_access': row[header_indices['written_during_early_access']]
                    }

                    review_data.append(review)

                    # Test smaller sizes first
                    #if (i >= 1 * batch_size):
                    #    break
                        
                        

end_time = time.perf_counter_ns()
print(f"Took {(end_time-start_time)/1000000} ms")