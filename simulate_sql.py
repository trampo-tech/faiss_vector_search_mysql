import numpy as np
import uuid # For generating unique IDs easily
import random # For random numbers
from datetime import datetime, timedelta

def generate_random_embedding(dimension=384):
    """Generates a random embedding vector."""
    return np.random.rand(dimension).astype(np.float32)

def emulate_rental_listings_db(num_listings=100, dimension=384):
    """
    Emulates a MySQL database table for rental listings.
    Generates dummy data with unique IDs, titles, descriptions,
    and placeholder embeddings.
    """
    listings_data = []
    base_titles = ["Cozy Apartment", "Spacious House", "Modern Studio",
                   "Family Home", "Loft in Downtown", "Beachfront Villa"]
    base_descriptions = [
        "Perfect for singles or couples, close to public transport.",
        "Large backyard, ideal for families with kids and pets.",
        "Minimalist design with great city views, all amenities included.",
        "Quiet neighborhood, walking distance to schools and parks.",
        "Vibrant area, steps away from restaurants and nightlife.",
        "Stunning ocean views, private access to the beach."
    ]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Miami", "Seattle"]
    types = ["apartment", "house", "condo", "studio", "villa"]
    prices = [1200.00, 1500.00, 1800.00, 2000.00, 2500.00, 3000.00, 3500.00, 4000.00]

    for i in range(num_listings):
        listing_id = i + 1 # Simple integer ID for emulation
        title = random.choice(base_titles) + " " + random.choice(cities)
        description = random.choice(base_descriptions)
        # Combine title and description to represent the text that would be embedded
        full_text = f"{title}. {description}"
        embedding = generate_random_embedding(dimension) # Placeholder embedding

        # Simulate timestamps
        created_at = datetime.now() - timedelta(days=random.randint(1, 365))
        updated_at = created_at + timedelta(days=random.randint(0, 30))
        last_embedding_generated_at = updated_at # Assume embedding is up-to-date initially

        listings_data.append({
            "id": listing_id,
            "title": title,
            "description": description,
            "city": random.choice(cities),
            "type": random.choice(types),
            "price": random.choice(prices),
            "embedding": embedding,
            "created_at": created_at,
            "updated_at": updated_at,
            "last_embedding_generated_at": last_embedding_generated_at
        })
    return listings_data

# Example Usage:
