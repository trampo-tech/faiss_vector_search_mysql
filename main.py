import fastapi
import faiss
from simulate_sql import emulate_rental_listings_db

my_mysql_emulation = emulate_rental_listings_db(num_listings=5)
#for listing in my_mysql_emulation:
#    print(f"ID: {listing['id']}, Title: {listing['title']}, Price: {listing['price']}, Embedding Shape: {listing['embedding'].shape}")
#print(f"\nTotal listings: {len(my_mysql_emulation)}")


from sentence_transformers import SentenceTransformer
sentences = ["This is an example sentence", "Each sentence is converted"]

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
embeddings = model.encode(sentences)
print(embeddings)
