import csv
import random
from datetime import datetime, timedelta

def random_date(start, end):
    """Generate a random date between start and end."""
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime('%Y-%m-%d')

first_names = ['John', 'Jane', 'Alex', 'Emily', 'Chris', 'Katie', 'Mike', 'Sara', 'Tom', 'Anna']
last_names = ['Doe', 'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Wilson', 'Taylor']

start_date = datetime(1970, 1, 1)
end_date = datetime(2005, 12, 31)

with open('users_03.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['username', 'email', 'first_name', 'last_name', 'birth_date'])
    for i in range(1_000_000):
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        username = f"{fn[0].lower()}{ln.lower()}{i}"
        email = f"{username}@example.com"
        birth_date = random_date(start_date, end_date)
        writer.writerow([username, email, fn, ln, birth_date])