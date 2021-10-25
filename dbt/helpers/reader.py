from cryptography.fernet import Fernet
import yaml
import os
      
def read_key(key, loc_k=f"{os.getenv('DBT_LOCKE')}"):

    if os.getenv('DT_MAINT') in os.getenv('USERPROFILE'):
        out_file = os.getenv('AH_CONFIG')
    else:
        out_file = f"{os.getenv('DT_CONFIG')}"

    with open(loc_k, 'r') as file:
        ab = yaml.safe_load(file)
    
    with open(out_file, 'r') as file:
        vals = yaml.safe_load(file)
        
        cryptor = Fernet(ab.get('passo')).decrypt(vals.get(key))
    
    return cryptor.decode()
