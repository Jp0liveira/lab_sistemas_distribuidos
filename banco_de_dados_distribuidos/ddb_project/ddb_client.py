"""
Cliente GUI para Banco de Dados Distribuído
Interface gráfica usando tkinter
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import socket
import json
import threading
import time
from datetime import datetime


class DDBClient:
    """Cliente para interagir com o DDB"""

    def __init__(self, master):
        self.master = master
        self.master.title("Cliente DDB - Banco de Dados Distribuído")
        self.master.geometry("1000x700")

        # Estado
        self.connected_node = None
        self.query_history = []

        self.create_widgets()

    def create_widgets(self):
        """Cria interface gráfica"""

        # Frame de conexão
        conn_frame = ttk.LabelFrame(self.master, text="Conexão", padding=10)
        conn_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(conn_frame, text="Host:").grid(row=0, column=0, padx=5)
        self.host_entry = ttk.Entry(conn_frame, width=20)
        self.host_entry.insert(0, "127.0.0.1")
        self.host_entry.grid(row=0, column=1, padx=5)

        ttk.Label(conn_frame, text="Port:").grid(row=0, column=2, padx=5)
        self.port_entry = ttk.Entry(conn_frame, width=10)
        self.port_entry.insert(0, "5000")
        self.port_entry.grid(row=0, column=3, padx=5)

        self.connect_btn = ttk.Button(
            conn_frame,
            text="Conectar",
            command=self.test_connection
        )
        self.connect_btn.grid(row=0, column=4, padx=5)

        self.status_label = ttk.Label(conn_frame, text="Desconectado", foreground="red")
        self.status_label.grid(row=0, column=5, padx=10)

        # Frame de query
        query_frame = ttk.LabelFrame(self.master, text="Editor de Query SQL", padding=10)
        query_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # Área de edição de query
        ttk.Label(query_frame, text="Digite sua query SQL:").pack(anchor=tk.W)

        self.query_text = scrolledtext.ScrolledText(
            query_frame,
            height=8,
            wrap=tk.WORD,
            font=("Courier", 11)
        )
        self.query_text.pack(fill=tk.BOTH, expand=True, pady=5)

        # Exemplos de queries
        examples = [
            "SELECT * FROM users;",
            "SELECT * FROM users WHERE id = 1;",
            "INSERT INTO users (name, email) VALUES ('João', 'joao@email.com');",
            "UPDATE users SET email = 'novo@email.com' WHERE id = 1;",
            "DELETE FROM users WHERE id = 1;"
        ]
        self.query_text.insert("1.0", "-- Exemplos de queries:\n")
        for ex in examples:
            self.query_text.insert(tk.END, f"-- {ex}\n")
        self.query_text.insert(tk.END, "\n-- Digite sua query abaixo:\n")

        # Botões de ação
        btn_frame = ttk.Frame(query_frame)
        btn_frame.pack(fill=tk.X, pady=5)

        self.execute_btn = ttk.Button(
            btn_frame,
            text="▶ Executar Query",
            command=self.execute_query,
            state=tk.DISABLED
        )
        self.execute_btn.pack(side=tk.LEFT, padx=5)

        ttk.Button(
            btn_frame,
            text="🗑 Limpar",
            command=self.clear_query
        ).pack(side=tk.LEFT, padx=5)

        self.query_type_var = tk.StringVar(value="READ")
        ttk.Radiobutton(
            btn_frame,
            text="SELECT (READ)",
            variable=self.query_type_var,
            value="READ"
        ).pack(side=tk.LEFT, padx=10)

        ttk.Radiobutton(
            btn_frame,
            text="INSERT/UPDATE/DELETE (WRITE)",
            variable=self.query_type_var,
            value="WRITE"
        ).pack(side=tk.LEFT, padx=10)

        # Frame de resultado
        result_frame = ttk.LabelFrame(self.master, text="Resultado da Query", padding=10)
        result_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.result_text = scrolledtext.ScrolledText(
            result_frame,
            height=8,
            wrap=tk.WORD,
            font=("Courier", 10),
            state=tk.DISABLED
        )
        self.result_text.pack(fill=tk.BOTH, expand=True)

        # Frame de informações
        info_frame = ttk.LabelFrame(self.master, text="Informações", padding=10)
        info_frame.pack(fill=tk.X, padx=10, pady=5)

        self.info_label = ttk.Label(info_frame, text="Aguardando execução de query...")
        self.info_label.pack(anchor=tk.W)

    def clear_query(self):
        """Limpa área de query"""
        self.query_text.delete("1.0", tk.END)

    def update_result(self, text, clear=True):
        """Atualiza área de resultado"""
        self.result_text.config(state=tk.NORMAL)
        if clear:
            self.result_text.delete("1.0", tk.END)
        self.result_text.insert(tk.END, text)
        self.result_text.config(state=tk.DISABLED)

    def test_connection(self):
        """Testa conexão com um nó"""
        host = self.host_entry.get()
        port = int(self.port_entry.get())

        self.update_result("Testando conexão...\n")

        try:
            # Envia heartbeat
            message = {
                'type': 'HEARTBEAT',
                'node_id': 'client',
                'timestamp': time.time(),
                'data': {'host': 'client', 'port': 0}
            }

            # Calcula checksum
            import hashlib
            checksum_data = message.copy()
            message['checksum'] = hashlib.md5(
                json.dumps(checksum_data, sort_keys=True).encode()
            ).hexdigest()

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((host, port))
            sock.send(json.dumps(message).encode())

            response = sock.recv(4096)
            sock.close()

            if response:
                self.connected_node = (host, port)
                self.status_label.config(text="Conectado", foreground="green")
                self.execute_btn.config(state=tk.NORMAL)
                self.update_result(f"✓ Conectado ao nó {host}:{port}\n")
                messagebox.showinfo("Sucesso", f"Conectado ao nó {host}:{port}")

        except Exception as e:
            self.status_label.config(text="Erro na conexão", foreground="red")
            self.update_result(f"✗ Erro ao conectar: {e}\n")
            messagebox.showerror("Erro", f"Não foi possível conectar:\n{e}")

    def execute_query(self):
        """Executa query no DDB"""
        if not self.connected_node:
            messagebox.showwarning("Aviso", "Conecte-se a um nó primeiro!")
            return

        query = self.query_text.get("1.0", tk.END).strip()

        # Remove comentários
        query_lines = []
        for line in query.split('\n'):
            line = line.strip()
            if line and not line.startswith('--'):
                query_lines.append(line)

        query = ' '.join(query_lines)

        if not query:
            messagebox.showwarning("Aviso", "Digite uma query SQL!")
            return

        self.execute_btn.config(state=tk.DISABLED)
        self.update_result("Executando query...\n")

        # Executa em thread separada
        threading.Thread(
            target=self._execute_query_thread,
            args=(query,),
            daemon=True
        ).start()

    def _execute_query_thread(self, query):
        """Executa query em thread separada"""
        try:
            host, port = self.connected_node

            # Cria mensagem
            message = {
                'type': 'QUERY',
                'node_id': 'client',
                'timestamp': time.time(),
                'data': {
                    'query': query,
                    'type': self.query_type_var.get()
                }
            }

            # Calcula checksum
            import hashlib
            checksum_data = message.copy()
            message['checksum'] = hashlib.md5(
                json.dumps(checksum_data, sort_keys=True).encode()
            ).hexdigest()

            # Envia query
            start_time = time.time()

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30)
            sock.connect((host, port))
            sock.send(json.dumps(message).encode())

            # Recebe resposta
            data = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
                if len(chunk) < 4096:
                    break

            sock.close()

            end_time = time.time()
            elapsed = end_time - start_time

            response = json.loads(data.decode())

            # Processa resultado
            result_text = f"{'=' * 60}\n"
            result_text += f"Query executada em: {elapsed:.3f}s\n"
            result_text += f"Nó que processou: {response['data'].get('node_id', 'Desconhecido')}\n"
            result_text += f"Status: {response['data']['status']}\n"
            result_text += f"{'=' * 60}\n\n"

            if response['data']['status'] == 'success':
                result_data = response['data']['result']

                if 'rows' in result_data:
                    rows = result_data['rows']
                    result_text += f"Registros retornados: {len(rows)}\n\n"

                    if rows:
                        # Formata tabela
                        if len(rows) > 0:
                            headers = list(rows[0].keys())

                            # Calcula largura das colunas
                            widths = {h: len(str(h)) for h in headers}
                            for row in rows:
                                for h in headers:
                                    widths[h] = max(widths[h], len(str(row.get(h, ''))))

                            # Cabeçalho
                            header_line = " | ".join(
                                str(h).ljust(widths[h]) for h in headers
                            )
                            result_text += header_line + "\n"
                            result_text += "-" * len(header_line) + "\n"

                            # Linhas
                            for row in rows[:100]:  # Limita a 100 linhas
                                line = " | ".join(
                                    str(row.get(h, '')).ljust(widths[h])
                                    for h in headers
                                )
                                result_text += line + "\n"

                            if len(rows) > 100:
                                result_text += f"\n... e mais {len(rows) - 100} registros\n"
                    else:
                        result_text += "Nenhum registro encontrado.\n"

                elif 'affected_rows' in result_data:
                    result_text += f"Linhas afetadas: {result_data['affected_rows']}\n"
                    result_text += "\n✓ Query executada com sucesso!\n"
                    result_text += "Os dados foram replicados em todos os nós do DDB.\n"

            else:
                result_text += f"✗ Erro: {response['data'].get('message', 'Erro desconhecido')}\n"

            # Atualiza interface
            self.master.after(0, self.update_result, result_text)
            self.master.after(0, self.info_label.config, {
                'text': f"Última query: {datetime.now().strftime('%H:%M:%S')} | "
                        f"Tempo: {elapsed:.3f}s | "
                        f"Nó: {response['data'].get('node_id', '?')}"
            })

            # Salva histórico
            self.query_history.append({
                'query': query,
                'timestamp': datetime.now().isoformat(),
                'elapsed': elapsed,
                'node': response['data'].get('node_id'),
                'status': response['data']['status']
            })

        except Exception as e:
            error_msg = f"✗ Erro ao executar query:\n{str(e)}\n"
            self.master.after(0, self.update_result, error_msg)
            self.master.after(0, messagebox.showerror, "Erro", str(e))

        finally:
            self.master.after(0, self.execute_btn.config, {'state': tk.NORMAL})


def main():
    root = tk.Tk()
    app = DDBClient(root)
    root.mainloop()


if __name__ == "__main__":
    main()