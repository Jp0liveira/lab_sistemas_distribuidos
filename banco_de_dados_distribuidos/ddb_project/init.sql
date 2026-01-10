-- Script de inicialização do banco de dados

-- Usa o banco
USE ddb_database;

-- Cria tabela de usuários
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_email (email),
    INDEX idx_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Insere dados iniciais
INSERT IGNORE INTO users (id, name, email) VALUES
    (1, 'Alice Silva', 'alice@example.com'),
    (2, 'Bob Santos', 'bob@example.com'),
    (3, 'Carol Oliveira', 'carol@example.com'),
    (4, 'David Costa', 'david@example.com'),
    (5, 'Eva Rodrigues', 'eva@example.com');

-- Cria tabela de produtos (para testes adicionais)
CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Insere produtos
INSERT IGNORE INTO products (id, name, price, stock) VALUES
    (1, 'Laptop Dell', 3500.00, 10),
    (2, 'Mouse Logitech', 150.00, 50),
    (3, 'Teclado Mecânico', 450.00, 25),
    (4, 'Monitor LG 24"', 800.00, 15),
    (5, 'Webcam HD', 250.00, 30);

-- Cria tabela de pedidos (relacionamento)
CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    total DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
    INDEX idx_user (user_id),
    INDEX idx_product (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Insere pedidos
INSERT IGNORE INTO orders (id, user_id, product_id, quantity, total) VALUES
    (1, 1, 1, 1, 3500.00),
    (2, 2, 2, 2, 300.00),
    (3, 3, 3, 1, 450.00);

-- View útil para relatórios
CREATE OR REPLACE VIEW order_details AS
SELECT
    o.id AS order_id,
    u.name AS customer_name,
    u.email AS customer_email,
    p.name AS product_name,
    o.quantity,
    p.price AS unit_price,
    o.total,
    o.created_at
FROM orders o
JOIN users u ON o.user_id = u.id
JOIN products p ON o.product_id = p.id;

-- Mostra resumo
SELECT 'Banco inicializado com sucesso!' AS Status;
SELECT COUNT(*) AS total_users FROM users;
SELECT COUNT(*) AS total_products FROM products;
SELECT COUNT(*) AS total_orders FROM orders;