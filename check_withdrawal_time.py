import requests
import argparse
import yaml
import json
import csv
import time
import sys
from datetime import datetime, timezone

# --- Helper Functions ---

def get_chain_config(beacon_url, session=None):
    """Fetches chain configuration (slots per epoch, seconds per slot)."""
    s = session or requests
    try:
        resp = s.get(f"{beacon_url}/eth/v1/config/spec")
        resp.raise_for_status()
        config = resp.json()['data']
        return {
            'SECONDS_PER_SLOT': int(config['SECONDS_PER_SLOT']),
            'SLOTS_PER_EPOCH': int(config['SLOTS_PER_EPOCH']),
            'GENESIS_TIME': None # Will fetch separately
        }
    except Exception as e:
        print(f"Error fetching chain config: {e}")
        return None

def get_genesis_time(beacon_url, session=None):
    """Fetches genesis time."""
    s = session or requests
    try:
        resp = s.get(f"{beacon_url}/eth/v1/beacon/genesis")
        resp.raise_for_status()
        return int(resp.json()['data']['genesis_time'])
    except Exception as e:
        print(f"Error fetching genesis time: {e}")
        return None

def fetch_validator_data(beacon_url, validator_identifier, session=None):
    """Fetches validator status and epoch info from beacon node."""
    s = session or requests
    try:
        # validator_identifier can be index or pubkey
        resp = s.get(f"{beacon_url}/eth/v1/beacon/states/head/validators/{validator_identifier}")
        resp.raise_for_status()
        data = resp.json()['data']
        return data
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return None # Validator not found
        print(f"HTTP Error fetching validator {validator_identifier}: {e}")
        return None
    except Exception as e:
        print(f"Error fetching validator {validator_identifier}: {e}")
        return None

def calculate_withdrawal_info(validator_data, genesis_time, slots_per_epoch, seconds_per_slot):
    """Calculates withdrawal timestamps based on validator data."""
    if not validator_data:
        return None

    validator = validator_data['validator']
    status = validator_data['status']
    index = validator_data['index']
    pubkey = validator['pubkey']
    
    exit_epoch = int(validator['exit_epoch'])
    withdrawable_epoch = int(validator['withdrawable_epoch'])
    
    # Constants for "Far Future" (never exited)
    # Usually ~1.8e19
    FAR_FUTURE = 100000000000 

    result = {
        'pubkey': pubkey,
        'index': index,
        'status': status,
        'withdrawable_epoch': withdrawable_epoch,
        'withdrawable_time_iso': None,
        'time_remaining': None,
        'is_withdrawable': False
    }

    if withdrawable_epoch > FAR_FUTURE:
        # Not yet withdrawable (active or not yet exited)
        result['withdrawable_epoch'] = "Pending"
        result['withdrawable_time_iso'] = "N/A"
        
        if exit_epoch < FAR_FUTURE:
             # In exit queue
             exit_timestamp = genesis_time + (exit_epoch * slots_per_epoch * seconds_per_slot)
             exit_dt = datetime.fromtimestamp(exit_timestamp, tz=timezone.utc)
             result['note'] = f"In Exit Queue. Est Exit: {exit_dt.isoformat()}"
        else:
             result['note'] = "Active (not exited)"
        
        return result

    # Calculate Timestamp
    withdrawal_timestamp = genesis_time + (withdrawable_epoch * slots_per_epoch * seconds_per_slot)
    withdrawal_dt = datetime.fromtimestamp(withdrawal_timestamp, tz=timezone.utc)
    
    result['withdrawable_time_iso'] = withdrawal_dt.isoformat()
    
    now = datetime.now(timezone.utc)
    time_remaining = withdrawal_dt - now
    
    if time_remaining.total_seconds() > 0:
        result['time_remaining'] = str(time_remaining)
        result['is_withdrawable'] = False
    else:
        result['time_remaining'] = "0:00:00"
        result['is_withdrawable'] = True
        result['note'] = "Eligible for sweep"

    # Add Effective Balance
    eff_bal_raw = int(validator.get('effective_balance', 0))
    # Conversion: 32000000000 = 1 GNO
    result['effective_balance_gno'] = eff_bal_raw / 32000000000

    return result

# --- File Loaders ---

def load_keys_from_yaml(yaml_path):
    """Parses the operators YAML file to extract all pubkeys."""
    keys = []
    try:
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
            if 'operators' not in data:
                print("Error: YAML file must contain 'operators' key.")
                return []
            
            for op in data['operators']:
                if 'keys' in op and isinstance(op['keys'], list):
                    keys.extend(op['keys'])
                else:
                    print(f"Warning: Operator {op.get('name', 'unknown')} has no keys list.")
    except Exception as e:
        print(f"Error reading YAML file: {e}")
        return []
    
    # Normalize keys
    return [k.lower() for k in keys]

def load_index_map_from_json(json_path):
    """Parses the JSON file to create a pubkey -> index map."""
    mapping = {}
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
            if 'validators' not in data:
                print("Error: JSON file must contain 'validators' list.")
                return {}
            
            for v in data['validators']:
                pubkey = v.get('pubkey', '').lower()
                index = v.get('index')
                if pubkey and index is not None:
                    mapping[pubkey] = index
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return {}
    return mapping

# --- Main Logic ---

def main():
    parser = argparse.ArgumentParser(description="Get ETH withdrawable time for validators")
    parser.add_argument("validator_id", nargs="?", help="Single Validator Index or Public Key")
    parser.add_argument("--yaml", help="Path to YAML file with operator keys")
    parser.add_argument("--json", help="Path to JSON file with validator indices")
    parser.add_argument("--out", default="withdrawal_times.csv", help="Output CSV file path (default: withdrawal_times.csv)")
    parser.add_argument("--node", default="http://localhost:5052", help="Beacon Node URL")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep between requests in seconds")

    args = parser.parse_args()

    # Setup Session
    session = requests.Session()

    # 1. Init Chain Config
    print(f"Connecting to node: {args.node}")
    config = get_chain_config(args.node, session)
    if not config:
        print("Failed to get chain config. Exiting.")
        return
    
    genesis_time = get_genesis_time(args.node, session)
    if not genesis_time:
        print("Failed to get genesis time. Exiting.")
        return

    slots_per_epoch = config['SLOTS_PER_EPOCH']
    seconds_per_slot = config['SECONDS_PER_SLOT']
    print(f"Chain Params: {slots_per_epoch} slots/epoch, {seconds_per_slot} sec/slot, Genesis: {genesis_time}")

    # 2. Determine Mode
    validators_to_check = [] # List of (identifier, original_pubkey_if_known)

    if args.validator_id:
        validators_to_check.append((args.validator_id, None))
    
    elif args.yaml:
        print(f"Reading keys from {args.yaml}...")
        pubkeys = load_keys_from_yaml(args.yaml)
        print(f"Found {len(pubkeys)} keys.")
        
        index_map = {}
        if args.json:
            print(f"Reading index map from {args.json}...")
            index_map = load_index_map_from_json(args.json)
            print(f"Loaded {len(index_map)} indices.")
        
        # Prepare list: use index if available, else pubkey
        skipped_count = 0
        for pk in pubkeys:
            if args.json:
                # If JSON map is provided, STRICTLY require the key to be in it
                if pk in index_map:
                    validators_to_check.append((index_map[pk], pk))
                else:
                    skipped_count += 1
            else:
                # Fallback if no JSON provided (though user requested skipping based on JSON, this handles the case where they forget the flag)
                validators_to_check.append((pk, pk))
        
        if skipped_count > 0:
            print(f"Skipped {skipped_count} keys not found in JSON map.")

    else:
        print("Error: Please provide either a validator_id or a --yaml file.")
        parser.print_help()
        return

    # 3. Process Validators
    results = []
    print(f"Processing {len(validators_to_check)} validators...")
    
    for i, (ident, original_pk) in enumerate(validators_to_check):
        if i > 0 and i % 10 == 0:
            print(f"Processed {i}/{len(validators_to_check)}...")
        
        val_data = fetch_validator_data(args.node, ident, session)
        
        if val_data:
            info = calculate_withdrawal_info(val_data, genesis_time, slots_per_epoch, seconds_per_slot)
            results.append(info)
        else:
            # Handle not found
            results.append({
                'pubkey': original_pk or ident,
                'withdrawable_epoch': 'Error/NotFound',
                'withdrawable_time_iso': 'Error/NotFound',
                'note': 'Validator not found on chain'
            })
        
        if args.sleep > 0:
            time.sleep(args.sleep)

    # 4. Output
    if args.yaml:
        # Batch mode -> CSV
        print(f"Writing results to {args.out}...")
        fieldnames = ['pubkey', 'withdrawable_epoch', 'withdrawable_time_iso', 'time_remaining', 'effective_balance_gno', 'status', 'note']
        
        try:
            with open(args.out, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for r in results:
                    # Filter keys to match fieldnames
                    row = {k: r.get(k, '') for k in fieldnames}
                    writer.writerow(row)
            print("Done.")
        except Exception as e:
            print(f"Error writing CSV: {e}")

    else:
        # Single mode -> Print to console
        if not results:
            print("No data found.")
        else:
            r = results[0]
            print(f"\n--- Withdrawal Info for {r.get('pubkey')} (Index: {r.get('index')}) ---")
            print(f"Status:             {r.get('status')}")
            print(f"Withdrawable Epoch: {r.get('withdrawable_epoch')}")
            print(f"Withdrawable Time:  {r.get('withdrawable_time_iso')}")
            if r.get('time_remaining'):
                print(f"Time Remaining:     {r.get('time_remaining')}")
            if r.get('note'):
                print(f"Note:               {r.get('note')}")

if __name__ == "__main__":
    main()
