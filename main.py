import fastapi
from simulate_sql import emulate_rental_listings_db
from faissManager import Faiss_Manager 

my_mysql_emulation = emulate_rental_listings_db(num_listings=5)
#for listing in my_mysql_emulation:
#    print(f"ID: {listing['id']}, Title: {listing['title']}, Price: {listing['price']}, Embedding Shape: {listing['embedding'].shape}")
#print(f"\nTotal listings: {len(my_mysql_emulation)}")

faiss_manager = Faiss_Manager(dimensionality=384)

faiss_manager.add_from_list(my_mysql_emulation)
text = "Family with Liberty Statue"

teste = faiss_manager.search_text(text)

my_mysql_emulation[2]