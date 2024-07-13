from git import Repo
import os

# Configurações de autenticação
github_username = "michellsspy"
github_token = "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"  # ou senha, dependendo da autenticação

# URL do repositório que você deseja clonar
repo_url = "https://github.com/michellsspy/motors-word.git"

# Diretório de destino para o clone
dest_dir = "/home/michel/Documentos/motors-word/data"

# Clonando o repositório
try:
    # Construindo a URL com as credenciais
    repo_url_with_creds = f"https://{github_username}:{github_token}@github.com/{repo_url.split('https://github.com/')[1]}"
    
    # Clonando o repositório
    Repo.clone_from(repo_url_with_creds, dest_dir)
    print("Repositório clonado com sucesso!")
except Exception as e:
    print(f"Erro ao clonar repositório: {str(e)}")
    
