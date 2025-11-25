# src/conversion/config_map.py

# This dictionary maps the RAW folder names to clean BIDS metadata.
# I have mapped them sequentially.
# NEED TO UPDATE: Check the 'sex' and 'details' for each mouse later.

SUBJECT_MAP = {
    # --- Double Injection Mice (DBL) ---
    "DBL_A": {
        "subject": "sub-dbl01",
        "session": "ses-01",
        "sex": "M", 
        "details": "Double Injection"
    },
    "DBL_C": {
        "subject": "sub-dbl02",
        "session": "ses-01",
        "sex": "F",
        "details": "Double Injection"
    },
    "DBL_D": {
        "subject": "sub-dbl03",
        "session": "ses-01",
        "sex": "M",
        "details": "Double Injection"
    },
    "DBL_E": {
        "subject": "sub-dbl04",
        "session": "ses-01",
        "sex": "F",
        "details": "Double Injection"
    },

    # --- Rabies Mice ---
    "RabiesA_Vglut1": {
        "subject": "sub-rab01",
        "session": "ses-01",
        "sex": "M",
        "details": "Rabies Vglut1"
    },
    "RabiesAA_Vglut1": {
        "subject": "sub-rab02",
        "session": "ses-01",
        "sex": "M",
        "details": "Rabies Vglut1"
    },
    "RabiesAB_Vglut1": {
        "subject": "sub-rab03",
        "session": "ses-01",
        "sex": "M",
        "details": "Rabies Vglut1"
    },
    "RabiesB_Vgat": {
        "subject": "sub-rab04",
        "session": "ses-01",
        "sex": "F",
        "details": "Rabies Vgat"
    },
    "RabiesBB_Vgat": {
        "subject": "sub-rab05",
        "session": "ses-01",
        "sex": "F",
        "details": "Rabies Vgat"
    },
    "RabiesD_Vgat": {
        "subject": "sub-rab06",
        "session": "ses-01",
        "sex": "M",
        "details": "Rabies Vgat"
    },
    "RabiesE_Vglut1": {
        "subject": "sub-rab07",
        "session": "ses-01",
        "sex": "F",
        "details": "Rabies Vglut1"
    }
}