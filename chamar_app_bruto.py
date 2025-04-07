import app_bruto

bi = app_bruto.BI()

li = [
    'izzy_plain_project',
    'days_off',
    'building_cost_estimation_items',
    'calendar',
    'enterprises',
    'companies',
    'income',
    'outcome',
   # 'by_bills',
    'izzy_plain_workflows',
    'izzy_plain_tasklogs'
]

for method_name in li:
    # OBTÉM O MÉTODO PELO NOME E CHAMA-O
    method = getattr(bi, method_name, None)
    if callable(method):
        try:
            method()  # CHAMA O MÉTODO
            print(f'{method_name} - Sucesso!!!')
            print('')
        except:
            print(f'{method_name} - Erro!!!')
            print('')
    else:
        print(f'Método {method_name} não encontrado ou não é chamável.')

