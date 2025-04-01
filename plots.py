import matplotlib.pyplot as plt

# Данные
x = [1, 2, 3, 4, 5]
y = [10, 20, 25, 30, 40]

# Создание графика
plt.plot(x, y, marker='o')

# Добавление заголовка и меток осей
plt.title('Пример графика')
plt.xlabel('Ось X')
plt.ylabel('Ось Y')

# Показать график
plt.grid()
plt.show()