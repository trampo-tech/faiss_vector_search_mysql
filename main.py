import fastapi
from simulate_sql import emulate_rental_listings_db
from faissManager import Faiss_Manager 

my_mysql_emulation = emulate_rental_listings_db(num_listings=1000)
#for listing in my_mysql_emulation:
#    print(f"ID: {listing['id']}, Title: {listing['title']}, Price: {listing['price']}, Embedding Shape: {listing['embedding'].shape}")
#print(f"\nTotal listings: {len(my_mysql_emulation)}")

faiss_manager = Faiss_Manager(dimensionality=300)

faiss_manager.add_from_list(my_mysql_emulation)
text = "Apartment New York son and daughter"

teste = faiss_manager.search_text(text)

best_match_id = teste[1][0][0]

my_mysql_emulation[best_match_id]