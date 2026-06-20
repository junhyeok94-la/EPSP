import unittest
from unittest.mock import MagicMock, patch
import json
import psycopg2
import sys
import os

# 모듈 탐색 경로 설정 (notebook/data-generator 디렉토리 접근용)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 테스트 타겟 모듈 import
import transaction_generator

class TestTransactionGenerator(unittest.TestCase):

    def setUp(self):
        # Faker 및 마스터 정보 백업/Mocking
        self.mock_producer = MagicMock()
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor

    @patch('transaction_generator.psycopg2.connect')
    def test_get_connection_success(self, mock_connect):
        mock_connect.return_value = self.mock_conn
        conn = transaction_generator.get_connection(retries=2, delay=0.01)
        self.assertEqual(conn, self.mock_conn)
        self.assertEqual(mock_connect.call_count, 1)

    @patch('transaction_generator.time.sleep')
    @patch('transaction_generator.psycopg2.connect')
    def test_get_connection_retry_and_fail(self, mock_connect, mock_sleep):
        mock_connect.side_effect = psycopg2.OperationalError("Connection lost")
        with self.assertRaises(psycopg2.OperationalError):
            transaction_generator.get_connection(retries=3, delay=0.01)
        self.assertEqual(mock_connect.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_initialize_db(self):
        # users 및 products 테이블 존재 체크 모킹
        self.mock_cursor.fetchone.side_effect = [("users",), ("products",)]
        
        success = transaction_generator.initialize_db(self.mock_conn)
        self.assertTrue(success)
        # INSERT 쿼리가 실행되었는지 검사
        self.mock_cursor.execute.assert_any_call(
            unittest.mock.ANY, 
            unittest.mock.ANY
        )
        self.mock_conn.commit.assert_called_once()

    def test_send_clickstream_event(self):
        user_id = "USR-0001"
        product_id = "PROD-001"
        event_type = "page_view"
        
        transaction_generator.send_clickstream_event(self.mock_producer, user_id, product_id, event_type)
        
        # produce가 올바른 아규먼트로 호출되었는지 확인
        self.mock_producer.produce.assert_called_once()
        args, kwargs = self.mock_producer.produce.call_args
        self.assertEqual(args[0], transaction_generator.CLICKSTREAM_TOPIC)
        self.assertEqual(kwargs['key'], user_id)
        
        # 페이로드 검증
        payload = json.loads(kwargs['value'])
        self.assertEqual(payload['user_id'], user_id)
        self.assertEqual(payload['product_id'], product_id)
        self.assertEqual(payload['event_type'], event_type)

    def test_send_order_stream_event(self):
        order_id = "ORD-TEST"
        user_id = "USR-0001"
        product_id = "PROD-001"
        quantity = 2
        total_price = 99.98
        status = "CREATED"
        
        transaction_generator.send_order_stream_event(
            self.mock_producer, order_id, user_id, product_id, quantity, total_price, status
        )
        
        self.mock_producer.produce.assert_called_once()
        args, kwargs = self.mock_producer.produce.call_args
        self.assertEqual(args[0], transaction_generator.ORDER_STREAM_TOPIC)
        self.assertEqual(kwargs['key'], user_id)
        
        payload = json.loads(kwargs['value'])
        self.assertEqual(payload['order_id'], order_id)
        self.assertEqual(payload['user_id'], user_id)
        self.assertEqual(payload['status'], status)
        self.assertEqual(payload['total_price'], total_price)

    def test_generate_order_success(self):
        # 재고 수량 10개 반환 모킹
        self.mock_cursor.fetchone.return_value = [10]
        
        # add_to_cart 확률을 1.0으로 고정시키기 위해 random.random 패치
        with patch('transaction_generator.random.random', return_value=0.1):
            transaction_generator.generate_order(self.mock_conn, self.mock_producer)
        
        # 트랜잭션 커밋이 일어났는지 확인
        self.mock_conn.commit.assert_called_once()
        # Kafka clickstream 3번 (page_view, add_to_cart, purchase) + order_stream 1번 (CREATED) = 4번 발행
        self.assertEqual(self.mock_producer.produce.call_count, 4)

    def test_generate_order_out_of_stock(self):
        # 재고 수량 0개 반환 모킹
        self.mock_cursor.fetchone.return_value = [0]
        
        transaction_generator.generate_order(self.mock_conn, self.mock_producer)
        
        # 롤백이 일어났는지 확인
        self.mock_conn.rollback.assert_called_once()
        # clickstream 이벤트 중 purchase와 order_stream은 가지 않았어야 함 (page_view와 add_to_cart만 감)
        self.assertLess(self.mock_producer.produce.call_count, 3)

    def test_simulate_cancellation(self):
        # 취소할 orders 1개 반환 모킹 (order_id, product_id, quantity, user_id, total_price)
        self.mock_cursor.fetchone.return_value = ("ORD-123", "PROD-001", 1, "USR-0001", 49.99)
        
        transaction_generator.simulate_cancellation(self.mock_conn, self.mock_producer)
        
        # 커밋이 일어났는지 확인
        self.mock_conn.commit.assert_called_once()
        # order_stream CANCELLED 발행 확인
        self.mock_producer.produce.assert_called_once()
        args, kwargs = self.mock_producer.produce.call_args
        self.assertEqual(args[0], transaction_generator.ORDER_STREAM_TOPIC)
        payload = json.loads(kwargs['value'])
        self.assertEqual(payload['status'], "CANCELLED")

if __name__ == '__main__':
    unittest.main()
