import json, sys
p = r'C:\Users\acer\clawd\skills\adv-qbo-tool\references\samples\category_mapping_fixture.xnofi.json'
d = json.load(open(p, 'r', encoding='utf-8'))
fx = d.get('fixture', [])
if not fx:
    print('FAIL: empty fixture')
    sys.exit(1)

missing = [x for x in fx if not x.get('category') or not x.get('account')]
if missing:
    print('FAIL: unmapped fixture rows', len(missing))
    sys.exit(1)

payroll = [x for x in fx if x.get('payment_type') == 'Payroll Payment- Xinofi']
if payroll and any(x.get('account') != '5501 Payroll - Head Office' for x in payroll):
    print('FAIL: payroll mapping drift')
    sys.exit(1)

print('PASS: fixture rows', len(fx))
