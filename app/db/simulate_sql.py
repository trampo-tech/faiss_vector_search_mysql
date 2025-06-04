import numpy as np
import random
from datetime import datetime, timedelta


def generate_random_embedding(dimension=384):
    """Generates a random embedding vector."""
    return np.random.rand(dimension).astype(np.float32)


def get_random_past_datetime():
    """Generates a random datetime within the last year."""
    days_to_subtract = random.randint(0, 365)
    random_date = datetime.now() - timedelta(days=days_to_subtract)
    return random_date.strftime("%Y-%m-%d %H:%M:%S")


def emulate_rental_listings_db(num_itens=100, num_users=10, dimension=384):
    """
    Emula uma tabela de banco de dados MySQL para itens de aluguel.
    Gera dados fictícios com IDs, nomes, descrições, categorias, etc.,
    e embeddings de placeholder.
    """
    itens_data = []
    nomes_base = [
        "Câmera Fotográfica DSLR",
        "Furadeira de Impacto",
        "Barraca de Camping Familiar",
        "Kit de Chaves de Fenda",
        "Caixa de Som Portátil",
        "Projetor Multimídia",
        "Violão Clássico",
        "Mochila de Trilha",
        "Jogo de Xadrez Profissional",
        "Bicicleta Urbana",
    ]
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
        "Leve e confortável para passeios na cidade.",
    ]
    categorias_base = [
        "Eletrônicos",
        "Ferramentas",
        "Esportes e Lazer",
        "Casa e Jardim",
        "Áudio e Vídeo",
        "Instrumentos Musicais",
        "Viagem",
        "Jogos",
    ]
    precos_base = [20.00, 30.00, 35.00, 45.00, 50.00, 60.00, 70.00, 80.00]
    condicoes_uso_base = [
        "Devolver limpo e nas mesmas condições.",
        "Uso exclusivo para fins pessoais.",
        "Não sublocar.",
        "Responsabilidade total por danos.",
        None,  # Simulating nullable field
        "Manusear com cuidado.",
    ]
    status_base = ["disponível", "alugado", "manutenção"]

    for i in range(num_itens):
        item_id = i + 1
        nome = random.choice(nomes_base) + " #" + str(random.randint(100, 999))
        descricao = random.choice(descricoes_base)
        embedding = generate_random_embedding(dimension)
        created_at_val = get_random_past_datetime()
        updated_at_val = (
            datetime.strptime(created_at_val, "%Y-%m-%d %H:%M:%S")
            + timedelta(days=random.randint(0, 30))
        ).strftime("%Y-%m-%d %H:%M:%S")

        itens_data.append(
            {
                "id": item_id,
                "titulo": nome,
                "descricao": descricao,
                "categoria": random.choice(categorias_base),
                "preco_diario": random.choice(precos_base),
                "condicoes_uso": random.choice(condicoes_uso_base),
                "status": random.choice(status_base),
                "usuario_id": random.randint(1, num_users),
                "embedding": embedding,
                "created_at": created_at_val,
                "updated_at": updated_at_val,
                "last_embedding_generated_at": updated_at_val,  # Assuming embedding is generated on update
            }
        )
    return itens_data


def emulate_users_db(num_users=10):
    """
    Emula uma tabela de banco de dados MySQL para usuários.
    Gera dados fictícios com IDs, nomes, emails, etc.
    """
    users_data = []
    nomes_ficticios = [
        "Ana Silva",
        "Bruno Costa",
        "Carlos Dias",
        "Daniela Lima",
        "Eduardo Reis",
        "Fernanda Alves",
        "Gustavo Borges",
        "Helena Matos",
        "Igor Ramos",
        "Julia Castro",
    ]
    dominios_email = ["example.com", "mailservice.net", "webmail.org"]

    for i in range(num_users):
        user_id = i + 1
        nome_completo = random.choice(nomes_ficticios)
        primeiro_nome = nome_completo.split(" ")[0].lower()
        email = (
            f"{primeiro_nome}{random.randint(1, 100)}@{random.choice(dominios_email)}"
        )
        created_at_val = get_random_past_datetime()
        updated_at_val = (
            datetime.strptime(created_at_val, "%Y-%m-%d %H:%M:%S")
            + timedelta(days=random.randint(0, 30))
        ).strftime("%Y-%m-%d %H:%M:%S")

        users_data.append(
            {
                "id": user_id,
                "nome": nome_completo,
                "email": email,
                "created_at": created_at_val,
                "updated_at": updated_at_val,
            }
        )
    return users_data
