import numpy as np
import random

def generate_random_embedding(dimension=384):
    """Generates a random embedding vector."""
    return np.random.rand(dimension).astype(np.float32)

def emulate_rental_listings_db(num_itens=100, dimension=384):
    """
    Emula uma tabela de banco de dados MySQL para itens de aluguel.
    Gera dados fictícios com IDs, nomes, descrições, categorias, etc.,
    e embeddings de placeholder.
    """
    itens_data = []
    nomes_base = ["Câmera Fotográfica DSLR", "Furadeira de Impacto", "Barraca de Camping Familiar",
                  "Kit de Chaves de Fenda", "Caixa de Som Portátil", "Projetor Multimídia", "Violão Clássico",
                  "Mochila de Trilha", "Jogo de Xadrez Profissional", "Bicicleta Urbana"]
    descricoes_base = [
        "Excelente para fotos de alta qualidade, acompanha lente 18-55mm.",
        "Potente e versátil para diversos tipos de trabalho.",
        "Espaçosa, ideal para 4 pessoas, resistente à água e fácil de montar.",
        "Jogo completo com diversos tamanhos e pontas.",
        "Som de alta fidelidade, conexão Bluetooth e bateria de longa duração.",
        "Resolução Full HD, ideal para filmes e apresentações.",
        "Cordas de nylon, sonoridade suave, perfeito para iniciantes.",
        "Capacidade 60L, confortável e com múltiplos compartimentos.",
        "Peças em madeira de lei, tabuleiro oficial.",
        "Leve e confortável para passeios na cidade."
    ]
    categorias_base = ["Eletrônicos", "Ferramentas", "Esportes e Lazer", "Casa e Jardim", "Áudio e Vídeo",
                       "Instrumentos Musicais", "Viagem", "Jogos"]
    precos_base = [20.00, 30.00, 35.00, 45.00, 50.00, 60.00, 70.00, 80.00]

    for i in range(num_itens):
        item_id = i + 1
        nome = random.choice(nomes_base) + " #" + str(random.randint(100, 999))
        descricao = random.choice(descricoes_base)
        embedding = generate_random_embedding(dimension) 

        itens_data.append({
            "id": item_id,
            "nome": nome,  
            "descricao": descricao,
            "categoria": random.choice(categorias_base),
            "preco_diaria": random.choice(precos_base), 
            "disponivel": random.choice([True, False]), 
            "embedding": embedding
        })
    return itens_data


