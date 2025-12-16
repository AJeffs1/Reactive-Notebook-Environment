# %% [id: 72a25438]
a = 5

# %% [id: 8205c5d7]
b = 5

# %% [id: ca98b2fe]
print(a+b)

# %% [id: 8f3b3432, type: sql, as: users_df]
CREATE TABLE IF NOT EXISTS test_users (
      id SERIAL PRIMARY KEY,
      name VARCHAR(100),
      score INT
  );
  
    INSERT INTO test_users (name, score) VALUES
      ('Alice', 85),
      ('Bob', 92),
      ('Charli', 78);

# %% [id: 72d72570, type: sql, as: users_df]
SELECT * FROM test_users ORDER BY score DESC

# %% [id: 90d9f433]
print(users_df)
_result = users_df
print(a)
