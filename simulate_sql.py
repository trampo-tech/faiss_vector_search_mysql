import numpy as np
import uuid 
import random 
from datetime import datetime, timedelta

def generate_random_embedding(dimension=384):
    """Generates a random embedding vector."""
    return np.random.rand(dimension).astype(np.float32)

def emulate_rental_listings_db(num_itens=100, dimension=384):
    """
    Emula uma tabela de banco de dados MySQL para itens de aluguel.
    Gera dados fictícios com IDs, títulos, descrições, categorias, etc.,
    e embeddings de placeholder.
    """
    itens_data = []
    titulos_base = ["Bicicleta Mountain Bike", "Console de Videogame", "Livro de Ficção Científica",
                    "Kit de Ferramentas Completo", "Caixa de Som Bluetooth", "Projetor HD", "Violão Acústico",
                    "Barraca de Camping", "Mala de Viagem Grande", "Jogo de Tabuleiro Moderno"]
    descricoes_base = [
        "Ideal para trilhas e aventuras ao ar livre, em ótimo estado.",
        "Última geração, com dois controles e jogos populares inclusos.",
        "Best-seller premiado, capa dura, como novo.",
        "Contém todas as ferramentas essenciais para reparos domésticos.",
        "Som potente e claro, bateria de longa duração, fácil de conectar.",
        "Perfeito para noites de cinema em casa ou apresentações.",
        "Cordas de nylon, ótimo para iniciantes e músicos experientes.",
        "Espaçosa para 4 pessoas, fácil de montar e resistente à água.",
        "Rígida e durável, com rodinhas 360º e segredo.",
        "Diversão garantida para toda a família e amigos."
    ]
    categorias_base = ["esportes", "eletronicos", "livros", "ferramentas", "audio_video", 
                       "instrumentos_musicais", "camping_aventura", "viagem", "jogos"]
    precos_base = [10.00, 15.00, 20.00, 25.00, 30.00, 40.00, 50.00, 60.00]
    condicoes_uso_base = [
        "Devolver no mesmo estado de conservação.",
        "Uso pessoal e recreativo apenas.",
        "Manusear com cuidado para evitar danos.",
        "Acessórios inclusos devem ser devolvidos.",
        "Multa por atraso na devolução."
    ]
    status_opcoes = ['disponivel', 'alugado', 'inativo']

    for i in range(num_itens):
        item_id = i + 1 
        titulo = random.choice(titulos_base) + " #" + str(random.randint(100, 999))
        descricao = random.choice(descricoes_base)
        embedding = generate_random_embedding(dimension) 

        
        created_at = datetime.now() - timedelta(days=random.randint(1, 365))
        updated_at = created_at + timedelta(days=random.randint(0, 30))
        last_embedding_generated_at = updated_at #

        itens_data.append({
            "id": item_id,
            "titulo": titulo,
            "descricao": descricao,
            "categoria": random.choice(categorias_base),
            "preco_diario": random.choice(precos_base),
            "condicoes_uso": random.choice(condicoes_uso_base),
            "status": random.choice(status_opcoes),
            "usuario_id": random.randint(1, 20), 
            "embedding": embedding,
            "created_at": created_at,
            "updated_at": updated_at,
            "last_embedding_generated_at": last_embedding_generated_at
        })
    return itens_data


