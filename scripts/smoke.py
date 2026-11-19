"""Authenticated integration checks for a running ReliefGrid API."""
import argparse
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx


def check(base_url, admin_user, admin_password):
    with httpx.Client(base_url=base_url, timeout=20) as client:
        def call(method, path, status=200, token=None, **kwargs):
            headers = {'Authorization': f'Bearer {token}'} if token else {}
            response = client.request(method, path, headers=headers, **kwargs)
            if response.status_code != status:
                raise AssertionError(f'{method} {path}: expected {status}, got '
                                     f'{response.status_code}: {response.text}')
            return response.json()

        def login(username, password):
            data = {'username': username, 'password': password}
            return call('POST', '/auth/login', json=data)['access_token']

        def create_user(role):
            username = f'smoke-{role}-{uuid.uuid4().hex}'
            password = uuid.uuid4().hex
            call('POST', '/auth/signup', 201,
                 json={'username': username, 'password': password, 'role': role})
            return login(username, password)

        def create_request(location):
            return call('POST', '/requests', 201, citizen,
                        json={'type': 'water', 'location': location, 'details': 'Smoke test'})

        def register_resource(location, kind='water'):
            return call('POST', '/resources', 201, volunteer,
                        json={'type': kind, 'location': location, 'availability': True})

        def statuses(ids):
            return [call('GET', f'/requests/{id}', token=citizen) for id in ids]

        def wait_for_match(ids):
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                records = statuses(ids)
                if any(r['status'] == 'MATCHED' for r in records):
                    return records
                time.sleep(0.5)
            raise AssertionError(f'No request matched within 30 seconds: {ids}')

        citizen = create_user('citizen')
        volunteer = create_user('volunteer')
        admin = login(admin_user, admin_password) if admin_user and admin_password else None
        call('GET', '/requests', 401)

        location = f'Smoke {uuid.uuid4().hex}'
        request_id = create_request(location)['id']
        register_resource(location, 'food')
        register_resource(location + ' other')
        assert statuses([request_id])[0]['status'] == 'PENDING'
        resource_id = register_resource(location.lower())['id']
        matched, = wait_for_match([request_id])
        assert matched['assigned_resource'] == resource_id
        assert call('GET', f'/requests/{request_id}', token=volunteer)['status'] == 'MATCHED'
        call('PUT', f'/resources/{resource_id}/status', 409, volunteer,
             json={'status': 'AVAILABLE'})

        second_location = location + ' parallel'
        register_resource(second_location)
        with ThreadPoolExecutor(max_workers=6) as pool:
            ids = [record['id'] for record in pool.map(create_request, [second_location] * 6)]
        wait_for_match(ids)
        time.sleep(2)
        assert sum(r['status'] == 'MATCHED' for r in statuses(ids)) == 1

        spare_id = register_resource(location + ' spare')['id']
        result = call('PUT', f'/resources/{spare_id}/status', token=volunteer,
                      json={'status': 'UNAVAILABLE'})
        assert result['availability'] is False

        if admin:
            call('PUT', f'/requests/{request_id}/status', 422, admin, json={'status': 'garbage'})
            call('POST', f'/match/{request_id}', token=admin)
            completed = call('PUT', f'/requests/{request_id}/status', token=admin,
                             json={'status': 'COMPLETED'})
            assert completed['status'] == 'COMPLETED'
            call('PUT', f'/resources/{resource_id}/status', token=volunteer,
                 json={'status': 'AVAILABLE'})

        print('Authenticated CRUD, RabbitMQ matching, and concurrent allocation passed.')
        if not admin:
            print('Admin checks skipped: set SMOKE_ADMIN_USER and SMOKE_ADMIN_PASSWORD.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://localhost:8000')
    args = parser.parse_args()
    check(args.url, os.getenv('SMOKE_ADMIN_USER'), os.getenv('SMOKE_ADMIN_PASSWORD'))

