"""Exercise the live payment-status handler with processor/database fakes only."""
import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock


class PaymentMeasurementTest(unittest.TestCase):
    def handler(self, payment):
        tree = ast.parse((Path(__file__).parents[1] / 'routes/pagos.py').read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'pago_estado')
        fn.decorator_list = []
        sdk = MagicMock()
        sdk.payment.return_value.get.return_value = {'response': payment}
        env = {'Request': object, 'Query': lambda default=None: default,
               '_MP_ACCESS_TOKEN': 'unit-test-only', 'mercadopago': SimpleNamespace(SDK=lambda _: sdk),
               'get_conn': MagicMock(), 'process_commission': MagicMock(), 'log': MagicMock(),
               'HTTPException': RuntimeError}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'pagos.py', 'exec'), env)
        return env['pago_estado'], sdk

    def test_verified_amount_is_not_the_plan_list_price(self):
        fn, sdk = self.handler({'id': 123, 'status': 'approved', 'transaction_amount': 129.5,
                                'currency_id': 'MXN', 'external_reference': 'company|cotizador'})
        result = fn(None, payment_id='123')
        self.assertTrue(result['paid'])
        self.assertTrue(result['verified'])
        self.assertEqual(result['value'], 129.5)
        self.assertEqual(result['currency'], 'MXN')
        self.assertEqual(result['payment_id'], '123')
        sdk.payment.return_value.get.assert_called_once_with(123)

    def test_rejected_payment_is_not_paid(self):
        fn, _ = self.handler({'id': 123, 'status': 'rejected', 'transaction_amount': 99, 'currency_id': 'MXN'})
        self.assertFalse(fn(None, payment_id='123')['paid'])

    def test_redirect_without_processor_verification_cannot_supply_revenue(self):
        fn, sdk = self.handler({})
        result = fn(None, collection_status='approved', external_reference='company|cotizador')
        self.assertNotIn('verified', result)
        self.assertNotIn('value', result)
        sdk.payment.assert_not_called()


if __name__ == '__main__':
    unittest.main()
