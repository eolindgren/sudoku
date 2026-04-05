from app import create_app
import traceback

try:
    app = create_app()
    with app.test_client() as client:
        response = client.get('/')
        print('Status:', response.status_code)
        if response.status_code != 200:
            print('Error:', response.data.decode())
        else:
            print('Success!')
except Exception as e:
    traceback.print_exc()
