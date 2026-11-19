import uuid

from locust import HttpUser, between, task


class Client(HttpUser):
    wait_time = between(0.5, 2)

    def on_start(self):
        self.citizen = self.token('citizen')
        self.volunteer = self.token('volunteer')

    def token(self, role):
        credentials = {
            'username': f'locust-{role}-{uuid.uuid4().hex}',
            'password': uuid.uuid4().hex,
        }
        self.client.post('/auth/signup', json={**credentials, 'role': role}).raise_for_status()
        response = self.client.post('/auth/login', json=credentials)
        response.raise_for_status()
        return {'Authorization': 'Bearer ' + response.json()['access_token']}

    @task(3)
    def create(self):
        self.client.post('/requests', headers=self.citizen,
                         json={'type': 'water', 'location': 'Bengaluru',
                               'details': 'Benchmark request'})

    @task(1)
    def resource(self):
        self.client.post('/resources', headers=self.volunteer,
                         json={'type': 'water', 'location': 'Bengaluru', 'availability': True})

