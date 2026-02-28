#!/usr/bin/env python3
"""
Cliente GUI para Banco de Dados Distribuído
Interface similar ao cliente de compilação Kotlin
"""

import tkinter as tk
from tkinter import scrolledtext, messagebox, ttk
import socket
import json
import hashlib
import random

class DDBClient:
    def __init__(self):
        self.janela = tk.Tk()
        self.janela.title("Cliente DDB - Banco de Dados Distribuído")
        self.janela.geometry("900x750")
        
        # Lista de nós disponíveis
        self.nodes = [
            {'id': 1, 'host': 'localhost', 'port': 5001},
            {'id': 2, 'host': 'localhost', 'port': 5002},
            {'id': 3, 'host': 'localhost', 'port': 5003}
        ]
        
        self.create_widgets()
    
    def calculate_checksum(self, data):
        """Calcula checksum MD5"""
        return hashlib.md5(data.encode('utf-8')).hexdigest()
    
    def select_node(self):
        """
        Seleciona um nó para executar a query (balanceamento de carga)
        Estratégia: Round-robin aleatório
        """
        return random.choice(self.nodes)
    
    def enviar_query(self):
        """Envia query SQL para o DDB"""
        query = self.txt_query.get("1.0", tk.END).strip()
        
        if not query:
            messagebox.showwarning("Aviso", "Digite uma query SQL!")
            return
        
        # Limpa áreas de resultado
        self.txt_resultado.config(state=tk.NORMAL)
        self.txt_resultado.delete("1.0", tk.END)
        self.txt_log.config(state=tk.NORMAL)
        self.txt_log.delete("1.0", tk.END)
        
        # Seleciona nó para executar
        node = self.select_node()
        
        self.log(f"→ Conectando ao Nó {node['id']} ({node['host']}:{node['port']})...")
        self.log(f"→ Query: {query[:80]}{'...' if len(query) > 80 else ''}\n")
        
        try:
            # Cria socket e conecta
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)
            s.connect((node['host'], node['port']))
            
            # Prepara mensagem
            checksum = self.calculate_checksum(query)
            message = {
                'type': 'QUERY',
                'query': query,
                'checksum': checksum
            }
            
            # Envia
            s.sendall(json.dumps(message).encode('utf-8'))
            self.log("✓ Query enviada com sucesso")
            
            # Recebe resposta
            self.log("→ Aguardando resposta...\n")
            resposta_raw = s.recv(8192).decode('utf-8')
            resposta = json.loads(resposta_raw)
            
            s.close()
            
            # Processa resposta
            self.processar_resposta(resposta, node)
            
        except socket.timeout:
            messagebox.showerror("Erro", "Timeout: Nó não respondeu a tempo")
            self.log("✗ ERRO: Timeout ao aguardar resposta")
        except ConnectionRefusedError:
            messagebox.showerror("Erro", f"Não foi possível conectar ao Nó {node['id']}")
            self.log(f"✗ ERRO: Conexão recusada pelo Nó {node['id']}")
        except json.JSONDecodeError as e:
            messagebox.showerror("Erro", "Resposta inválida do servidor")
            self.log(f"✗ ERRO: Resposta JSON inválida: {e}")
        except Exception as e:
            messagebox.showerror("Erro de Conexão", f"Erro: {e}")
            self.log(f"✗ ERRO: {e}")
    
    def processar_resposta(self, resposta, node):
        """Processa e exibe a resposta do servidor"""
        status = resposta.get('status')
        
        if status == 'success':
            self.log("✓ Query executada com SUCESSO!\n")
            
            # Informações do nó
            node_id = resposta.get('node_id', node['id'])
            self.log(f"Nó executante: {node_id}")
            
            # Se foi query de modificação
            if 'rows_affected' in resposta:
                rows_affected = resposta['rows_affected']
                self.log(f"Linhas afetadas: {rows_affected}")
                
                # Informações de replicação
                if 'replicated' in resposta:
                    rep = resposta['replicated']
                    self.log(f"Replicação: {rep['success_count']}/{rep['total_peers']} nós")
                    
                    if rep['failed_peers']:
                        self.log(f"⚠ Falha na replicação: Nós {rep['failed_peers']}")
                
                self.txt_resultado.insert(tk.END, f"✓ Operação executada com sucesso!\n")
                self.txt_resultado.insert(tk.END, f"Linhas afetadas: {rows_affected}\n")
                self.txt_resultado.insert(tk.END, f"Nó: {node_id}\n")
            
            # Se foi query SELECT
            elif 'data' in resposta:
                data = resposta['data']
                row_count = resposta.get('row_count', len(data))
                
                self.log(f"Registros retornados: {row_count}\n")
                
                if row_count == 0:
                    self.txt_resultado.insert(tk.END, "Nenhum registro encontrado.\n")
                else:
                    # Exibe resultados
                    self.txt_resultado.insert(tk.END, f"Registros encontrados: {row_count}\n")
                    self.txt_resultado.insert(tk.END, f"Nó executante: {node_id}\n")
                    self.txt_resultado.insert(tk.END, "\n" + "="*70 + "\n\n")
                    
                    # Formata como tabela
                    for i, row in enumerate(data, 1):
                        self.txt_resultado.insert(tk.END, f"Registro {i}:\n")
                        for key, value in row.items():
                            self.txt_resultado.insert(tk.END, f"  {key}: {value}\n")
                        self.txt_resultado.insert(tk.END, "\n")
        
        elif status == 'error':
            error_msg = resposta.get('message', 'Erro desconhecido')
            self.log(f"✗ ERRO: {error_msg}")
            
            self.txt_resultado.insert(tk.END, f"ERRO:\n{error_msg}\n")
            self.txt_resultado.tag_add("error", "1.0", "end")
            self.txt_resultado.tag_config("error", foreground="red")
        
        self.txt_resultado.config(state=tk.DISABLED)
        self.txt_log.config(state=tk.DISABLED)
    
    def verificar_status(self):
        """Verifica status de todos os nós"""
        self.txt_log.config(state=tk.NORMAL)
        self.txt_log.delete("1.0", tk.END)
        
        self.log("Verificando status dos nós...\n")
        
        for node in self.nodes:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(3)
                s.connect((node['host'], node['port']))
                
                message = {'type': 'STATUS'}
                s.sendall(json.dumps(message).encode('utf-8'))
                
                resposta = s.recv(4096).decode('utf-8')
                status = json.loads(resposta)
                
                s.close()
                
                coord = " [COORDENADOR]" if status.get('is_coordinator') else ""
                self.log(f"✓ Nó {node['id']}: ATIVO{coord}")
                self.log(f"  - Queries processadas: {status.get('queries_processed', 0)}")
                self.log(f"  - Peers: {status.get('peers_count', 0)}\n")
                
            except Exception as e:
                self.log(f"✗ Nó {node['id']}: OFFLINE ou ERRO ({e})\n")
        
        self.txt_log.config(state=tk.DISABLED)
    
    def log(self, mensagem):
        """Adiciona mensagem ao log"""
        self.txt_log.insert(tk.END, mensagem + "\n")
        self.txt_log.see(tk.END)
    
    def inserir_exemplo(self, tipo):
        """Insere exemplo de query"""
        exemplos = {
            'create': """CREATE TABLE IF NOT EXISTS usuarios (
    id INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE,
    data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);""",
            'insert': """INSERT INTO usuarios (nome, email) 
VALUES ('João Silva', 'joao@email.com');""",
            'select': """SELECT * FROM usuarios;""",
            'update': """UPDATE usuarios 
SET nome = 'João Paulo Silva' 
WHERE id = 1;""",
            'delete': """DELETE FROM usuarios WHERE id = 1;"""
        }
        
        self.txt_query.delete("1.0", tk.END)
        self.txt_query.insert("1.0", exemplos.get(tipo, ''))
    
    def create_widgets(self):
        """Cria interface gráfica"""
        
        # Frame superior - Configurações
        frame_config = tk.Frame(self.janela, bg="#2c3e50", pady=10)
        frame_config.pack(fill=tk.X)
        
        tk.Label(
            frame_config, 
            text="🗄️ Cliente de Banco de Dados Distribuído",
            font=("Arial", 14, "bold"),
            bg="#2c3e50",
            fg="white"
        ).pack()
        
        # Frame de exemplos
        frame_exemplos = tk.Frame(self.janela, pady=10)
        frame_exemplos.pack(fill=tk.X)
        
        tk.Label(frame_exemplos, text="Exemplos rápidos:", font=("Arial", 9)).pack(side=tk.LEFT, padx=10)
        
        tk.Button(frame_exemplos, text="CREATE", command=lambda: self.inserir_exemplo('create'), width=8).pack(side=tk.LEFT, padx=2)
        tk.Button(frame_exemplos, text="INSERT", command=lambda: self.inserir_exemplo('insert'), width=8).pack(side=tk.LEFT, padx=2)
        tk.Button(frame_exemplos, text="SELECT", command=lambda: self.inserir_exemplo('select'), width=8).pack(side=tk.LEFT, padx=2)
        tk.Button(frame_exemplos, text="UPDATE", command=lambda: self.inserir_exemplo('update'), width=8).pack(side=tk.LEFT, padx=2)
        tk.Button(frame_exemplos, text="DELETE", command=lambda: self.inserir_exemplo('delete'), width=8).pack(side=tk.LEFT, padx=2)
        
        # Área de Query
        tk.Label(
            self.janela, 
            text="Digite sua query SQL:", 
            font=("Arial", 10, "bold")
        ).pack(anchor="w", padx=10, pady=(10, 0))
        
        self.txt_query = scrolledtext.ScrolledText(self.janela, height=10, font=("Courier", 10))
        self.txt_query.pack(padx=10, pady=5, fill=tk.BOTH, expand=False)
        
        # Query padrão
        self.txt_query.insert(tk.INSERT, "SELECT * FROM usuarios;")
        
        # Botões de ação
        frame_buttons = tk.Frame(self.janela, pady=10)
        frame_buttons.pack()
        
        btn_executar = tk.Button(
            frame_buttons,
            text="🚀 EXECUTAR QUERY",
            bg="#27ae60",
            fg="white",
            font=("Arial", 12, "bold"),
            command=self.enviar_query,
            width=20,
            height=2
        )
        btn_executar.pack(side=tk.LEFT, padx=5)
        
        btn_status = tk.Button(
            frame_buttons,
            text="📊 STATUS DOS NÓS",
            bg="#3498db",
            fg="white",
            font=("Arial", 12, "bold"),
            command=self.verificar_status,
            width=20,
            height=2
        )
        btn_status.pack(side=tk.LEFT, padx=5)
        
        # Área de Resultado
        tk.Label(
            self.janela,
            text="Resultado da Query:",
            font=("Arial", 10, "bold")
        ).pack(anchor="w", padx=10, pady=(10, 0))
        
        self.txt_resultado = scrolledtext.ScrolledText(
            self.janela,
            height=10,
            bg="#f0f0f0",
            font=("Courier", 9)
        )
        self.txt_resultado.pack(padx=10, pady=5, fill=tk.BOTH, expand=True)
        self.txt_resultado.config(state=tk.DISABLED)
        
        # Área de Log
        tk.Label(
            self.janela,
            text="Log de Comunicação:",
            font=("Arial", 10, "bold"),
            fg="#2980b9"
        ).pack(anchor="w", padx=10, pady=(10, 0))
        
        self.txt_log = scrolledtext.ScrolledText(
            self.janela,
            height=8,
            bg="#ecf0f1",
            font=("Courier", 8)
        )
        self.txt_log.pack(padx=10, pady=5, fill=tk.X)
        self.txt_log.config(state=tk.DISABLED)
        
        # Rodapé
        footer = tk.Label(
            self.janela,
            text="Sistema DDB com Replicação Automática | Algoritmo Bully | Propriedades ACID",
            font=("Arial", 8),
            fg="#7f8c8d"
        )
        footer.pack(pady=5)
    
    def run(self):
        """Inicia a aplicação"""
        self.janela.mainloop()


if __name__ == '__main__':
    app = DDBClient()
    app.run()
