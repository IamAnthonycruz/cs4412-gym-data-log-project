"""
Strength Training Data Parser
Parses 2 years of workout logs into analysis-ready CSV files.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import os
from pathlib import Path
from collections import defaultdict

# Configuration
DATA_DIR = Path(r"C:\Users\cruzi\OneDrive\Desktop\GYM Data")
OUTPUT_DIR = DATA_DIR

# ============================================================================
# EXERCISE NAME STANDARDIZATION MAPPING
# ============================================================================
EXERCISE_MAPPING = {
    # Chest - Bench Press variations
    "DB bench": "Dumbbell Bench Press",
    "db bench": "Dumbbell Bench Press",
    "+DB bench": "Dumbbell Bench Press (Paused)",
    "DB BP": "Dumbbell Bench Press",
    "+DB BP": "Dumbbell Bench Press (Paused)",
    "DB bench press": "Dumbbell Bench Press",
    "+DB bench press": "Dumbbell Bench Press (Paused)",
    "BB BP": "Barbell Bench Press",
    "+BB BP": "Barbell Bench Press (Paused)",
    "Bench": "Bench Press",
    "Bench press": "Bench Press",
    "BP": "Bench Press",
    "Cambered bar BP": "Cambered Bar Bench Press",

    # Chest - Smith Machine variations
    "Incline SM CG BP": "Smith Machine Incline Close Grip Bench Press",
    "+Incline SM CG BP": "Smith Machine Incline Close Grip Bench Press (Paused)",
    "SM incline CG BP": "Smith Machine Incline Close Grip Bench Press",
    "+SM incline CG BP": "Smith Machine Incline Close Grip Bench Press (Paused)",
    "SM CG incline BP": "Smith Machine Incline Close Grip Bench Press",
    "SM incline press": "Smith Machine Incline Press",
    "+SM incline press": "Smith Machine Incline Press (Paused)",
    "SM incline chest press": "Smith Machine Incline Press",
    "+SM incline chest press": "Smith Machine Incline Press (Paused)",
    "Incline SM incline press": "Smith Machine Incline Press",
    "+Incline SM incline press": "Smith Machine Incline Press (Paused)",
    "Incline smith": "Smith Machine Incline Press",
    "CG SM BP": "Smith Machine Close Grip Bench Press",
    "+CG SM BP": "Smith Machine Close Grip Bench Press (Paused)",
    "SM CG BP": "Smith Machine Close Grip Bench Press",
    "+SM CG BP": "Smith Machine Close Grip Bench Press (Paused)",
    "SM CG": "Smith Machine Close Grip Bench Press",
    "+SM CG": "Smith Machine Close Grip Bench Press (Paused)",
    "CG BP": "Close Grip Bench Press",
    "+CG BP": "Close Grip Bench Press (Paused)",
    "+CGB": "Close Grip Bench Press (Paused)",
    "+SM BP": "Smith Machine Bench Press (Paused)",
    "+SM CGBP": "Smith Machine Close Grip Bench Press (Paused)",
    "+CG SM incline press": "Smith Machine Incline Close Grip Bench Press (Paused)",
    "Flat smith": "Smith Machine Flat Bench Press",

    # Chest - Machine Presses
    "Panatta chest press": "Panatta Chest Press",
    "+Panatta chest press": "Panatta Chest Press (Paused)",
    "Panatta Chest press": "Panatta Chest Press",
    "+Panatta Chest press": "Panatta Chest Press (Paused)",
    "Panatta CG": "Panatta Close Grip Press",
    "+Panatta CG": "Panatta Close Grip Press (Paused)",
    "Panatta CG press": "Panatta Close Grip Press",
    "+Panatta CG press": "Panatta Close Grip Press (Paused)",
    "+Pannata CG press": "Panatta Close Grip Press (Paused)",
    "HS incline chest press": "Hammer Strength Incline Chest Press",
    "+HS incline chest press": "Hammer Strength Incline Chest Press (Paused)",
    "HS Incline chest press": "Hammer Strength Incline Chest Press",
    "+HS Incline chest press": "Hammer Strength Incline Chest Press (Paused)",
    "HS incline": "Hammer Strength Incline Chest Press",
    "Incline HS chest press": "Hammer Strength Incline Chest Press",
    "Strive chest press": "Strive Chest Press",
    "+Strive chest press": "Strive Chest Press (Paused)",
    "+Strive Chest press": "Strive Chest Press (Paused)",
    "+Strive chest": "Strive Chest Press (Paused)",
    "Strive incline": "Strive Incline Press",
    "+Strive incline": "Strive Incline Press (Paused)",
    "Strive incline chest press": "Strive Incline Press",
    "+Strive incline chest press": "Strive Incline Press (Paused)",
    "+Strive Incline chest press": "Strive Incline Press (Paused)",
    "AFS incline": "AFS Incline Press",
    "+AFS incline": "AFS Incline Press (Paused)",
    "AFS Incline": "AFS Incline Press",
    "AFS incline smith": "AFS Smith Incline Press",
    "+AFS incline smith": "AFS Smith Incline Press (Paused)",
    "AFS Incline smith": "AFS Smith Incline Press",
    "+AFS incline smith machine": "AFS Smith Incline Press (Paused)",
    "+AFS incline smith machine press": "AFS Smith Incline Press (Paused)",
    "+AFS SM incline": "AFS Smith Incline Press (Paused)",
    "AFS SM Incline chest press": "AFS Smith Incline Press",
    "LVG chest press": "Leverage Chest Press",
    "+LVG chest press": "Leverage Chest Press (Paused)",
    "+Leverage chest press": "Leverage Chest Press (Paused)",
    "Magnum incline bench": "Magnum Incline Bench Press",
    "Magnum incline": "Magnum Incline Bench Press",
    "+Nat incline": "Nautilus Incline Press (Paused)",
    "+Prime incline": "Prime Incline Press (Paused)",
    "+Incline BP": "Incline Bench Press (Paused)",

    # Chest - JM Press
    "JM press": "JM Press",
    "+JM press": "JM Press (Paused)",
    "JM Press": "JM Press",
    "+JM Press": "JM Press (Paused)",
    "BB JM": "JM Press",
    "+SM JM press": "Smith Machine JM Press (Paused)",

    # Chest - Flyes
    "Cable flies": "Cable Fly",
    "Incline fly": "Incline Fly",
    "Incline Fly": "Incline Fly",
    "Incline chest fly": "Incline Fly",
    "Chest fly": "Chest Fly",
    "Cable fly": "Cable Fly",
    "Natilus fly": "Nautilus Fly",
    "Nat fly": "Nautilus Fly",
    "Flex incline fly": "Flex Incline Fly",
    "HI fly": "High Incline Fly",
    "+HI fly": "High Incline Fly (Paused)",
    "+BF fly": "Flat Fly (Paused)",
    "+BM fly": "Fly (Paused)",
    "BM fly": "Fly",
    "BM Fly": "Fly",
    "Arsenal fly": "Arsenal Fly",
    "BF pec fly": "Flat Pec Fly",
    "Prime fly": "Prime Fly",
    "Prime pec deck": "Prime Pec Deck",
    "+Prime pec deck": "Prime Pec Deck (Paused)",
    "Body masters pec deck": "Body Masters Pec Deck",
    "Strive pec fly": "Strive Pec Fly",

    # Shoulders
    "Legend shoulder": "Legend Shoulder Press",
    "+Legend shoulder": "Legend Shoulder Press (Paused)",
    "Legend Shoulder": "Legend Shoulder Press",
    "+Legend Shoulder": "Legend Shoulder Press (Paused)",
    "LVG shoulder": "Leverage Shoulder Press",
    "+LVG shoulder": "Leverage Shoulder Press (Paused)",
    "DB OHP": "Dumbbell Overhead Press",
    "+DB OHP": "Dumbbell Overhead Press (Paused)",
    "Seated DB OHP": "Seated Dumbbell Overhead Press",
    "DB overhead press": "Dumbbell Overhead Press",
    "DB shoulder": "Dumbbell Shoulder Press",
    "+DB shoulder": "Dumbbell Shoulder Press (Paused)",
    "DB shoulder press": "Dumbbell Shoulder Press",
    "AD press": "Atlantis Deltoid Press",
    "AD Press": "Atlantis Deltoid Press",
    "+AD press": "Atlantis Deltoid Press (Paused)",
    "Nat shoulder": "Nautilus Shoulder Press",
    "+Nat shoulder": "Nautilus Shoulder Press (Paused)",
    "Nat shoulder press": "Nautilus Shoulder Press",
    "+Natilus shoulder": "Nautilus Shoulder Press (Paused)",
    "HS Shoulder": "Hammer Strength Shoulder Press",
    "+HS Shoulder": "Hammer Strength Shoulder Press (Paused)",
    "HS shoulder": "Hammer Strength Shoulder Press",
    "HS OHP": "Hammer Strength Overhead Press",
    "Machine shoulder": "Machine Shoulder Press",
    "+Machine shoulder": "Machine Shoulder Press (Paused)",
    "+Shoulder machine": "Machine Shoulder Press (Paused)",
    "BTN press": "Behind The Neck Press",
    "+BTN press": "Behind The Neck Press (Paused)",
    "+BTN smith": "Behind The Neck Smith Press (Paused)",
    "Kloklov press": "Klokov Press",
    "BhKloklov press": "Klokov Press",
    "SM AD press": "Smith Machine Deltoid Press",

    # Shoulders - Lateral Raises
    "Cybex lateral raise": "Cybex Lateral Raise",
    "Cybex lateral": "Cybex Lateral Raise",
    "Cybex Lateral": "Cybex Lateral Raise",
    "Cybex lat raise": "Cybex Lateral Raise",
    "Cybex Lat raise": "Cybex Lateral Raise",
    "Cybex Lateral raises": "Cybex Lateral Raise",
    "Cybdx lateral": "Cybex Lateral Raise",
    "Cybex eagle lateral": "Cybex Lateral Raise",
    "SA Cybex lat": "Cybex Single Arm Lateral Raise",
    "Icarian Lateral": "Icarian Lateral Raise",
    "Icarian lateral": "Icarian Lateral Raise",
    "Icarian Lateral raise": "Icarian Lateral Raise",
    "Icarian Lateral raises": "Icarian Lateral Raise",
    "Icarian super lateral": "Icarian Lateral Raise (Super ROM)",
    "Super ROM Icarian lateral": "Icarian Lateral Raise (Super ROM)",
    "Icarian Super ROM lateral": "Icarian Lateral Raise (Super ROM)",
    "Avenger lateral": "Avenger Lateral Raise",
    "+Avenger lateral": "Avenger Lateral Raise (Paused)",
    "Lat raise": "Lateral Raise",
    "Lat raises": "Lateral Raise",
    "Lateral raise": "Lateral Raise",
    "Lateral": "Lateral Raise",
    "Laterals": "Lateral Raise",
    "Super ROM Lateral raises": "Lateral Raise (Super ROM)",
    "DB lateral": "Dumbbell Lateral Raise",
    "Atlantis lateral": "Atlantis Lateral Raise",
    "PTTA lateral": "Lateral Raise",
    "Med X lateral": "MedX Lateral Raise",

    # Shoulders - Rear Delts
    "Rear delt fly": "Rear Delt Fly",
    "RDF": "Rear Delt Fly",
    "Rear delt": "Rear Delt Fly",
    "Natilus Rear delt": "Nautilus Rear Delt",
    "Face pulls": "Face Pull",
    "Face pull": "Face Pull",

    # Shoulders - Upright Rows / Shrugs
    "Upright row": "Upright Row",
    "Upright Row": "Upright Row",
    "Upright rows": "Upright Row",
    "SM Upright row": "Smith Machine Upright Row",
    "SM Upright rows": "Smith Machine Upright Row",
    "Shrugs": "Shrug",
    "Shrug": "Shrug",
    "Shrugs//neck": "Shrug / Neck",
    "Shrugs and neck": "Shrug / Neck",
    "Power Shrugs": "Power Shrug",
    "Power shrugs": "Power Shrug",
    "Power shrug": "Power Shrug",
    "Kelso Shrugs": "Kelso Shrug",

    # Back - Rows
    "BB rows": "Barbell Row",
    "Natilus SA row": "Nautilus Single Arm Row",
    "Natilus row": "Nautilus Row",
    "Nebula row": "Nebula Row",
    "Nebula Row": "Nebula Row",
    "HS low row": "Hammer Strength Low Row",
    "HS Low row": "Hammer Strength Low Row",
    "Low Row": "Low Row",
    "Low row": "Low Row",
    "HS row": "Hammer Strength Row",
    "+HS Low row": "Hammer Strength Low Row (Paused)",
    "Strive row": "Strive Row",
    "Strive low row": "Strive Low Row",
    "SA cybex row": "Cybex Single Arm Row",
    "SA Cybex row": "Cybex Single Arm Row",
    "Cybex SA eagle": "Cybex Single Arm Eagle Row",
    "Cybex SA row": "Cybex Single Arm Row",
    "Cybex eagle SA": "Cybex Single Arm Eagle Row",
    "Cybex row": "Cybex Row",
    "Cybex eagle row": "Cybex Eagle Row",
    "Cybex eagle row(KG)": "Cybex Eagle Row",
    "SA Cybex Eagle row": "Cybex Single Arm Eagle Row",
    "SA Cybex Eagle row(KG)": "Cybex Single Arm Eagle Row",
    "SA Cybex eagle row": "Cybex Single Arm Eagle Row",
    "SA Cybex eagle row(KG)": "Cybex Single Arm Eagle Row",
    "Cybex Eagle row SA(KG)": "Cybex Single Arm Eagle Row",
    "+Cybex Eagle row (kg)": "Cybex Eagle Row (Paused)",
    "Cybex Mid row": "Cybex Mid Row",
    "Nat NG Seated row": "Nautilus Neutral Grip Seated Row",
    "Nat NG row": "Nautilus Neutral Grip Row",
    "Avenger row": "Avenger Row",
    "Avenger Row": "Avenger Row",
    "+Avenger row": "Avenger Row (Paused)",
    "Avenger rowJM": "Avenger Row",
    "Hoist row": "Hoist Row",
    "Extreme row": "Extreme Row",
    "Lat row": "Lat Row",
    "Lat cable row": "Cable Lat Row",
    "NG cable row": "Neutral Grip Cable Row",
    "cable row": "Cable Row",
    "Cable row": "Cable Row",
    "Pendlay row": "Pendlay Row",
    "Tbar": "T-Bar Row",
    "T bar": "T-Bar Row",
    "Arsenal tbar": "Arsenal T-Bar Row",
    "+LF row": "Lat Focused Row (Paused)",
    "LF row": "Lat Focused Row",
    "1. LF row": "Lat Focused Row",

    # Back - Pulldowns/Pull-ups
    "SWG Pull ups": "Supinated Wide Grip Pull-up",
    "SWG pullups": "Supinated Wide Grip Pull-up",
    "SWG pull ups": "Supinated Wide Grip Pull-up",
    "SWG pulldown": "Supinated Wide Grip Pulldown",
    "SWG Pulldown": "Supinated Wide Grip Pulldown",
    "SWG Lat pull": "Supinated Wide Grip Pulldown",
    "Natilus pulldown": "Nautilus Pulldown",
    "Nat pulldown": "Nautilus Pulldown",
    "nat pulldown": "Nautilus Pulldown",
    "Nat Pulldown": "Nautilus Pulldown",
    "SA Natilus pulldown": "Nautilus Single Arm Pulldown",
    "SA Nat pulldown": "Nautilus Single Arm Pulldown",
    "Nat SA pulldown": "Nautilus Single Arm Pulldown",
    "Natilus SA pulldown": "Nautilus Single Arm Pulldown",
    "SA nat pulldown": "Nautilus Single Arm Pulldown",
    "Nat SA Nat pulldown": "Nautilus Single Arm Pulldown",
    "SA Nat Pulldown": "Nautilus Single Arm Pulldown",
    "Nat SA": "Nautilus Single Arm Pulldown",
    "FM Pulldown": "Freemotion Pulldown",
    "NG Pull ups": "Neutral Grip Pull-up",
    "NG pull ups": "Neutral Grip Pull-up",
    "NG Pull up": "Neutral Grip Pull-up",
    "NG pull down": "Neutral Grip Pulldown",
    "NG pull": "Neutral Grip Pull-up",
    "WG pulldown": "Wide Grip Pulldown",
    "WG Pulldown": "Wide Grip Pulldown",
    "WG Pull down": "Wide Grip Pulldown",
    "WG lat pull": "Wide Grip Lat Pulldown",
    "WG pull ups": "Wide Grip Pull-up",
    "WG pullups": "Wide Grip Pull-up",
    "LF pulldown": "Lat Focused Pulldown",
    "LF WG pulldown": "Lat Focused Wide Grip Pulldown",
    "Pulldown": "Pulldown",
    "Chins": "Chin-up",
    "Chin": "Chin-up",
    "Chin ups": "Chin-up",
    "Pull ups": "Pull-up",
    "Pull": "Pull-up",
    "Pull 2": "Pull-up",
    "WG pull ups": "Wide Grip Pull-up",
    "HS Pulldown": "Hammer Strength Pulldown",
    "+HS Pulldown": "Hammer Strength Pulldown (Paused)",
    "HS SA pulldown": "Hammer Strength Single Arm Pulldown",
    "+HS SA pulldown": "Hammer Strength Single Arm Pulldown (Paused)",
    "SA HS Pulldown": "Hammer Strength Single Arm Pulldown",
    "SA HS pulldown": "Hammer Strength Single Arm Pulldown",
    "+SA Hammer pulldown": "Hammer Single Arm Pulldown (Paused)",
    "Magnum Pulldown": "Magnum Pulldown",
    "Magnum pulldown": "Magnum Pulldown",
    "+BFS pulldown": "Pulldown (Paused)",
    "BFS pulldown": "Pulldown",

    # Back - Pullovers
    "Dorian pullover": "Dorian Pullover",
    "Dorian Pullover": "Dorian Pullover",
    "Pullover": "Pullover",
    "+Pullover": "Pullover (Paused)",
    "Pull over": "Pullover",
    "Lat Pullover": "Lat Pullover",
    "DB Pullover": "Dumbbell Pullover",
    "+DB pullover": "Dumbbell Pullover (Paused)",
    "DB pullover": "Dumbbell Pullover",
    "Nat pullover": "Nautilus Pullover",
    "+Nat pullover": "Nautilus Pullover (Paused)",
    "Natilus pullover": "Nautilus Pullover",
    "Nat super pullover": "Nautilus Super Pullover",
    "Yates pullover": "Yates Pullover",
    "+Yates pullover": "Yates Pullover (Paused)",

    # Back - Deadlifts
    "Deadlifts": "Deadlift",
    "DL": "Deadlift",
    "DLs": "Deadlift",
    "Dls": "Deadlift",
    "RDLs": "Romanian Deadlift",
    "RDL": "Romanian Deadlift",
    "+RDLs": "Romanian Deadlift (Paused)",
    "+RDL": "Romanian Deadlift (Paused)",
    "+BL RDL": "Bilateral Romanian Deadlift (Paused)",
    "+BL RDLS": "Bilateral Romanian Deadlift (Paused)",

    # Legs - Squats
    "Cybex hack": "Cybex Hack Squat",
    "Cybex Hack": "Cybex Hack Squat",
    "Cybex Hack squat": "Cybex Hack Squat",
    "Cybex hback": "Cybex Hack Squat",
    "Hack": "Hack Squat",
    "Hack squat": "Hack Squat",
    "+Arsenal hack": "Arsenal Hack Squat (Paused)",
    "SSB squat": "Safety Squat Bar Squat",
    "+SSB squat": "Safety Squat Bar Squat (Paused)",
    "SSB lunge": "Safety Squat Bar Lunge",
    "+B SSB squat": "Safety Squat Bar Squat (Paused)",
    "+HB squat": "High Bar Squat (Paused)",
    "HB squat": "High Bar Squat",
    "HB Squat": "High Bar Squat",
    "HB BS": "High Bar Back Squat",
    "+HE SSB squat": "Heels Elevated Safety Squat Bar Squat (Paused)",
    "+HB SM squat": "High Bar Smith Machine Squat (Paused)",
    "+Hatfield squat": "Hatfield Squat (Paused)",
    "Belt squat": "Belt Squat",
    "+Belt squat": "Belt Squat (Paused)",
    "HI Smith": "High Incline Smith",
    "HI smith": "High Incline Smith",
    "+SM HI": "Smith Machine High Incline (Paused)",

    # Legs - Leg Press
    "Leg press": "Leg Press",
    "+Leg press": "Leg Press (Paused)",
    "Cybex leg press": "Cybex Leg Press",
    "Cybex Leg press": "Cybex Leg Press",
    "+Cybex leg press": "Cybex Leg Press (Paused)",
    "+Cybdx Leg press": "Cybex Leg Press (Paused)",
    "SL pendulum leg press": "Single Leg Pendulum Leg Press",
    "+SL pendulum leg press": "Single Leg Pendulum Leg Press (Paused)",
    "Pendulum SL leg press": "Single Leg Pendulum Leg Press",
    "+Pendulum SL leg press": "Single Leg Pendulum Leg Press (Paused)",
    "+SL Pendulum leg press": "Single Leg Pendulum Leg Press (Paused)",
    "+SL Pendulm Leg press": "Single Leg Pendulum Leg Press (Paused)",
    "+Pendulum SL Leg press": "Single Leg Pendulum Leg Press (Paused)",
    "+Pendulum leg press": "Pendulum Leg Press (Paused)",
    "+SA Pendulum leg press": "Single Arm Pendulum Leg Press (Paused)",
    "+SL Leg press": "Single Leg Press (Paused)",
    "+SL pendulum": "Single Leg Pendulum Press (Paused)",
    "2S+ Pendulum SA Leg press": "Single Arm Pendulum Leg Press",
    "LP": "Leg Press",

    # Legs - Leg Extensions
    "Leg extensions": "Leg Extension",
    "Leg extension": "Leg Extension",
    "Leg Extensions": "Leg Extension",
    "+Leg extensions": "Leg Extension (Paused)",
    "Strive Leg extensions": "Strive Leg Extension",
    "Strive leg extensions": "Strive Leg Extension",
    "Strive Leg extension": "Strive Leg Extension",
    "Strive leg extension": "Strive Leg Extension",
    "Strive Legs extensions": "Strive Leg Extension",
    "+Strive Leg extensions": "Strive Leg Extension (Paused)",
    "+Strive leg extension": "Strive Leg Extension (Paused)",
    "+Strive Leg ext": "Strive Leg Extension (Paused)",
    "Strive Leg ext": "Strive Leg Extension",
    "SL Leg Extension": "Single Leg Extension",
    "SL Leg extension": "Single Leg Extension",
    "SL Leg extensions": "Single Leg Extension",
    "Strive SL Leg ext": "Strive Single Leg Extension",
    "+Strive SL Leg ext": "Strive Single Leg Extension (Paused)",
    "+Strive SL Leg extensions": "Strive Single Leg Extension (Paused)",
    "Strive SL Leg extensions": "Strive Single Leg Extension",
    "+SL Strive Leg ext": "Strive Single Leg Extension (Paused)",
    "+SL Strive Leg extensions": "Strive Single Leg Extension (Paused)",
    "SL Strive Leg extensions": "Strive Single Leg Extension",
    "SA Strive Leg extensions": "Strive Single Arm Leg Extension",
    "(4) +Strive SL Leg ext": "Strive Single Leg Extension (Paused)",
    "LF Leg extensions": "Leg Extension",
    "PL Strive Leg extensions": "Strive Leg Extension",
    "+SA Prime Leg extensions": "Prime Single Arm Leg Extension (Paused)",
    "+SA prime leg ext": "Prime Single Arm Leg Extension (Paused)",
    "SL prime leg ext": "Prime Single Leg Extension",
    "+SL prime leg ext": "Prime Single Leg Extension (Paused)",
    "Prime SA Leg extension": "Prime Single Arm Leg Extension",

    # Legs - Leg Curls
    "Med X Leg curls": "MedX Leg Curl",
    "+Med X Leg curls": "MedX Leg Curl (Paused)",
    "+Med X leg curls": "MedX Leg Curl (Paused)",
    "+MedX Leg curls": "MedX Leg Curl (Paused)",
    "+Medx Leg curls": "MedX Leg Curl (Paused)",
    "Leg curls": "Leg Curl",
    "Leg curl": "Leg Curl",
    "Leg cuels": "Leg Curl",
    "+Leg curls": "Leg Curl (Paused)",
    "Seated Leg curls": "Seated Leg Curl",
    "+Seated leg curls": "Seated Leg Curl (Paused)",
    "+Seated Leg curls": "Seated Leg Curl (Paused)",
    "Seated leg curls": "Seated Leg Curl",
    "Lying Leg curl": "Lying Leg Curl",
    "Lying Leg curls": "Lying Leg Curl",
    "SL Leg curls": "Single Leg Curl",
    "+SL Leg curls": "Single Leg Curl (Paused)",
    "+SL Leg curl": "Single Leg Curl (Paused)",
    "+SL leg curl": "Single Leg Curl (Paused)",
    "S Leg curl": "Single Leg Curl",
    "SL Ham curl": "Single Leg Hamstring Curl",
    "SA Leg curls": "Single Arm Leg Curl",
    "+SA leg curl": "Single Arm Leg Curl (Paused)",
    "Flex SL Leg curls": "Flex Single Leg Curl",
    "+Flex SL Leg curls": "Flex Single Leg Curl (Paused)",
    "+Flex SL Leg curl": "Flex Single Leg Curl (Paused)",
    "Avenger leg curls": "Avenger Leg Curl",
    "+Avenger leg curls": "Avenger Leg Curl (Paused)",
    "Pendulum Leg curl": "Pendulum Leg Curl",
    "Pendulum leg curl": "Pendulum Leg Curl",
    "Pendulum leg curls": "Pendulum Leg Curl",
    "+Strive SL Leg curls": "Strive Single Leg Curl (Paused)",
    "+Paused leg curls": "Leg Curl (Paused)",

    # Legs - Other
    "Calf raises": "Calf Raise",
    "Calf raise": "Calf Raise",
    "Calf Raise": "Calf Raise",
    "+Calf raise": "Calf Raise (Paused)",
    "+Calf": "Calf Raise (Paused)",
    "Calf press": "Calf Press",
    "Calfs": "Calf Raise",
    "Calves": "Calf Raise",
    "Icarian calf": "Icarian Calf Raise",
    "LF Calf raises": "Calf Raise",
    "Nebula calf": "Nebula Calf Raise",
    "Seated calf": "Seated Calf Raise",
    "Atlantic abductor": "Atlantis Abductor",
    "Atlantis abductor": "Atlantis Abductor",
    "Abductor": "Abductor",
    "Abductors": "Abductor",
    "Abductr": "Abductor",
    "Nebula Abductor": "Nebula Abductor",
    "Hip thrust": "Hip Thrust",
    "BB Hip thrust": "Barbell Hip Thrust",
    "BB hip thrusts": "Barbell Hip Thrust",
    "BB HT cable": "Barbell Hip Thrust Cable",
    "Leg raise": "Leg Raise",
    "Leg Raises": "Leg Raise",

    # Arms - Tricep Pushdowns
    "Tricep pushdown": "Tricep Pushdown",
    "Trcp pushdown": "Tricep Pushdown",
    "trcp pushdown": "Tricep Pushdown",
    "+Trcp pushdown": "Tricep Pushdown (Paused)",
    "Pushdown": "Tricep Pushdown",
    "Pushdowns": "Tricep Pushdown",
    "Cybex trcp pushdown": "Cybex Tricep Pushdown",
    "Cybex Trcp pushdown": "Cybex Tricep Pushdown",
    "Cybex pushdown": "Cybex Tricep Pushdown",
    "Natilus pushdown": "Nautilus Tricep Pushdown",
    "Natilus Pushdown": "Nautilus Tricep Pushdown",
    "Natilus SA pushdown": "Nautilus Single Arm Tricep Pushdown",
    "Natilus Trcp pushdown": "Nautilus Tricep Pushdown",
    "Nat Pushdown": "Nautilus Tricep Pushdown",
    "FG pushdown": "Forward Grip Pushdown",
    "FG Trcp pushdown": "Forward Grip Tricep Pushdown",
    "FG trcp pushdown": "Forward Grip Tricep Pushdown",
    "Rope pushdown": "Rope Tricep Pushdown",
    "SA pushdown": "Single Arm Tricep Pushdown",
    "SA Trcp pushdown": "Single Arm Tricep Pushdown",
    "SA rope pushdown": "Single Arm Rope Pushdown",
    "Supported pushdown": "Supported Tricep Pushdown",
    "Supported SA tricep pushdowns": "Supported Single Arm Tricep Pushdown",

    # Arms - Tricep Extensions
    "Trcp OVH ext": "Tricep Overhead Extension",
    "OVH ext": "Overhead Extension",
    "OVH Ext machine": "Overhead Extension Machine",
    "OVH ext machine": "Overhead Extension Machine",
    "Paramount OVH ext": "Paramount Overhead Extension",
    "Trcp OVH ext machine": "Tricep Overhead Extension Machine",
    "Rope OVH": "Rope Overhead Extension",
    "Rope OVH ext": "Rope Overhead Extension",
    "HS ext": "Hammer Strength Extension",
    "SA HS extension": "Hammer Strength Single Arm Extension",

    # Arms - Skull Crushers
    "DB Skull crushers": "Dumbbell Skull Crusher",
    "DB skulls": "Dumbbell Skull Crusher",
    "+DB skulls": "Dumbbell Skull Crusher (Paused)",
    "Skull crushers": "Skull Crusher",
    "Skulls": "Skull Crusher",
    "skulls": "Skull Crusher",
    "+Skulls": "Skull Crusher (Paused)",
    "+skulls": "Skull Crusher (Paused)",
    "Skulls//RC": "Skull Crusher / Reverse Curl",
    "Skulls//RVS Curls": "Skull Crusher / Reverse Curl",
    "EZ skulls": "EZ Bar Skull Crusher",
    "EZ bar skulls": "EZ Bar Skull Crusher",

    # Arms - Dips
    "Dips": "Dip",
    "+Dips": "Dip (Paused)",
    "Dips mchin": "Dip Machine",
    "Dip machine": "Dip Machine",
    "Machine dip": "Dip Machine",
    "Super ROM Dips": "Super ROM Dip",
    "+Flex dip": "Flex Dip (Paused)",
    "+Flex machine dip": "Flex Machine Dip (Paused)",
    "+Pulse Dip": "Pulse Dip (Paused)",
    "+Pulse dip": "Pulse Dip (Paused)",
    "+Pulse dips": "Pulse Dip (Paused)",
    "+Pulse Mchn dip": "Pulse Machine Dip (Paused)",

    # Arms - Bicep Curls
    "DB curls": "Dumbbell Bicep Curl",
    "Seated DB curls": "Seated Dumbbell Bicep Curl",
    "Incline curls": "Incline Bicep Curl",
    "Cybex preacher curls": "Cybex Preacher Curl",
    "Cybex preacher": "Cybex Preacher Curl",
    "Cybex Preacher": "Cybex Preacher Curl",
    "Bi Cybex Preacher": "Cybex Preacher Curl",
    "SA Cybex preacher": "Cybex Single Arm Preacher Curl",
    "Preacher": "Preacher Curl",
    "Preacher curls": "Preacher Curl",
    "BB Preacher curls": "Barbell Preacher Curl",
    "Preacher hammer": "Preacher Hammer Curl",
    "Supported preacher": "Supported Preacher Curl",
    "Hammer curls": "Hammer Curl",
    "Hammer Curls": "Hammer Curl",
    "DB hammer curls": "Dumbbell Hammer Curl",
    "Hammers": "Hammer Curl",
    "Hammer": "Hammer Curl",
    "hammer": "Hammer Curl",
    "DB Hammer": "Dumbbell Hammer Curl",
    "DB hammer": "Dumbbell Hammer Curl",
    "DB hammers": "Dumbbell Hammer Curl",
    "BB Hammers": "Barbell Hammer Curl",
    "BB hammer": "Barbell Hammer Curl",
    "SF Hammers": "Hammer Curl",
    "Seated Hammers": "Seated Hammer Curl",
    "Seated DB hammers": "Seated Dumbbell Hammer Curl",
    "Supported Hammer": "Supported Hammer Curl",
    "Supported hammer": "Supported Hammer Curl",
    "Supported Hammer curls": "Supported Hammer Curl",
    "Supported hammer curls": "Supported Hammer Curl",
    "Supported Hammers": "Supported Hammer Curl",
    "SA Supported hammer": "Supported Single Arm Hammer Curl",
    "Cable Hammer curls": "Cable Hammer Curl",
    "Rope hammer curls": "Rope Hammer Curl",
    "Rope Hammer curls": "Rope Hammer Curl",
    "Rope hammer curls//SS//Rope OVH": "Rope Hammer Curl SS Rope Overhead",
    "RHC": "Rope Hammer Curl",
    "Bayesian curls": "Bayesian Curl",
    "Bayesian Curls": "Bayesian Curl",
    "BB curls": "Barbell Curl",
    "Bb curls": "Barbell Curl",
    "EZ bar curls": "EZ Bar Curl",
    "EZ Bar curls": "EZ Bar Curl",
    "Ez bar curls": "EZ Bar Curl",
    "EZ bars curls": "EZ Bar Curl",
    "EZ Curls": "EZ Bar Curl",
    "EZ bar": "EZ Bar Curl",
    "Cable curls": "Cable Curl",
    "Reverse curls": "Reverse Curl",
    "RVS curls": "Reverse Curl",
    "Rvs": "Reverse Curl",
    "Rvs Curls": "Reverse Curl",

    # Core
    "HS crunch": "Hammer Strength Crunch",
    "Med X crunch": "MedX Crunch",
    "+Med X crunch": "MedX Crunch (Paused)",
    "+Med X Crunch": "MedX Crunch (Paused)",
    "MedX crunch": "MedX Crunch",
    "Avenger crunch": "Avenger Crunch",
    "Avenger abs crunch": "Avenger Crunch",
    "Abs crunch": "Ab Crunch",
    "Ab crunch": "Ab Crunch",
    "Ab curl": "Ab Curl",
    "Crunch": "Crunch",
    "crunch": "Crunch",
    "Natilus crunch": "Nautilus Crunch",
    "nat crunch": "Nautilus Crunch",
    "Abs": "Ab Crunch",
    "Ab rower": "Ab Rower",
    "Abs rower": "Ab Rower",
    "Flex flexor": "Flex Hip Flexor",
    "Ab flexor": "Ab Flexor",
    "Abflexor": "Ab Flexor",
    "Obliques": "Obliques",

    # Neck
    "Neck curls": "Neck Curl",
    "Neck curl": "Neck Curl",
    "Neck": "Neck Curl",

    # Misc
    "+Red Smith": "Smith Press (Paused)",
    "Arms and legs": "Arms and Legs",
    "BM cable": "Cable",

    # Additional missing mappings
    "+SA Nat pulldown": "Nautilus Single Arm Pulldown (Paused)",
    "+Strive shoulder": "Strive Shoulder Press (Paused)",
    "+Pendulum SL Press": "Single Leg Pendulum Press (Paused)",
    "icarian lateral": "Icarian Lateral Raise",
    "isolation": "Isolation",

    # Additional exercises
    "Cybex eagle": "Cybex Eagle Row",
    "Nat OHP": "Nautilus Overhead Press",
    "Nat Pullover": "Nautilus Pullover",
    "Nat SA Pulldown": "Nautilus Single Arm Pulldown",
    "SA": "Single Arm",
    "SM HI": "Smith Machine High Incline",
    "FRC xDB bench": "Dumbbell Bench Press",

    # Ignore invalid entries (numbers/patterns that got parsed as exercise names)
    "1": None,
    "490/3 BO+RDLs": None,
    "505/3 BO +RDLs": None,
}

# Exercises that use actual barbell (add 45lb bar weight)
BARBELL_EXERCISES = {
    "Deadlift", "Romanian Deadlift", "Romanian Deadlift (Paused)",
    "Barbell Row", "Safety Squat Bar Squat", "Safety Squat Bar Squat (Paused)",
    "JM Press", "JM Press (Paused)", "Skull Crusher", "Skull Crusher (Paused)",
    "Dumbbell Skull Crusher", "Barbell Curl", "EZ Bar Curl",
    "Upright Row", "Barbell Preacher Curl",
    "Smith Machine Incline Close Grip Bench Press",
    "Smith Machine Incline Close Grip Bench Press (Paused)",
    "Smith Machine Incline Press", "Smith Machine Incline Press (Paused)",
    "Smith Machine Close Grip Bench Press",
    "Smith Machine Close Grip Bench Press (Paused)",
    "AFS Smith Incline Press", "AFS Smith Incline Press (Paused)",
}

# ============================================================================
# PARSING FUNCTIONS
# ============================================================================

def parse_weight_notation(weight_str, exercise_standard, exercise_raw=""):
    """
    Parse weight notation and return (weight_lbs, is_bodyweight, has_extender).

    Handles:
    - PPs notation: 3PPs+30 -> plates per side calculation
    - BW+ notation: BW+25 -> bodyweight plus weight
    - FS notation: FS, FS+10 -> Full Stack (returns None for weight)
    - KG notation: 103.5KG -> convert to lbs
    - Direct numbers: 90 -> direct weight
    - Fractions: Unicode fractions like 7.5
    """
    if not weight_str or weight_str.strip() in ['NA', 'MATCH', 'GF', 'BF', 'DL', '']:
        return None, False, False, weight_str

    weight_str = str(weight_str).strip()
    original = weight_str
    has_extender = False
    is_bodyweight = False

    # Check if exercise name indicates KG (for cases like "Cybex eagle row(KG)")
    exercise_is_kg = 'KG' in exercise_raw.upper() or 'kg' in exercise_raw

    # Replace unicode fractions
    weight_str = weight_str.replace('⅛', '.125').replace('¼', '.25').replace('⅓', '.333')
    weight_str = weight_str.replace('½', '.5').replace('⅔', '.667').replace('¾', '.75')
    weight_str = weight_str.replace('⅚', '.833').replace('⅝', '.625').replace('⅜', '.375')

    # Handle FS (Full Stack) notation
    if weight_str.startswith('FS'):
        has_extender = '+' in weight_str
        return None, False, has_extender, original

    # Handle BW (bodyweight) notation - use 185 lbs as standard bodyweight
    STANDARD_BODYWEIGHT = 185
    if weight_str.startswith('BW'):
        is_bodyweight = True
        match = re.search(r'BW\+?(\d+\.?\d*)', weight_str)
        if match:
            added_weight = float(match.group(1))
            return STANDARD_BODYWEIGHT + added_weight, True, False, original
        return STANDARD_BODYWEIGHT, True, False, original

    # Handle KG notation - convert to lbs (multiply by 2.205)
    kg_match = re.search(r'(\d+\.?\d*)\s*\+?\s*(\d+\.?\d*)?\s*KG', weight_str, re.IGNORECASE)
    if kg_match or 'KG' in weight_str.upper():
        # Extract all numbers before KG
        numbers = re.findall(r'(\d+\.?\d*)', weight_str.split('KG')[0].split('kg')[0])
        if numbers:
            # Sum all the numbers (handles cases like "94.5+10KG")
            total_kg = sum(float(n) for n in numbers if n)
            total_lbs = total_kg * 2.205
            return round(total_lbs, 1), False, False, original
        return None, False, False, original

    # Handle PPs (plates per side) notation
    pps_match = re.match(r'(\d+)PPs?(?:\+(\d+\.?\d*))?', weight_str)
    if pps_match:
        plates = int(pps_match.group(1))
        extra = float(pps_match.group(2)) if pps_match.group(2) else 0

        # Calculate weight: plates * 45 * 2 sides + extra
        weight = (plates * 45 * 2) + extra

        # Add bar weight for barbell exercises (45 lb bar)
        # Add machine base weight for machines (22.5 lbs)
        if exercise_standard in BARBELL_EXERCISES:
            weight += 45
        else:
            weight += 22.5  # Machine base weight

        return weight, False, False, original

    # Handle direct numbers (possibly with + for extender)
    if '+' in weight_str and not weight_str.startswith('+'):
        # Check if this is an extender notation like "150/13+3"
        # But weight_str here is just the weight part
        pass

    # Try to extract a number
    # For KG exercises, sum all numbers (handles cases like "103.5+10")
    if exercise_is_kg:
        numbers = re.findall(r'(\d+\.?\d*)', weight_str)
        if numbers:
            total_kg = sum(float(n) for n in numbers if n)
            weight = round(total_kg * 2.205, 1)
            return weight, False, False, original

    num_match = re.match(r'\+?(\d+\.?\d*)', weight_str)
    if num_match:
        weight = float(num_match.group(1))
        return weight, False, False, original

    return None, False, False, original


def parse_reps_notation(reps_str):
    """
    Parse reps notation and return (reps, has_extender).

    Handles:
    - Simple numbers: 8 -> 8
    - Decimals/partials: 7.5 -> 7.5
    - Extenders: 13+3 -> 16, has_extender=True
    - Multiple sets: 8x2 -> handled at set level
    - Single leg: 6:4 -> returns tuple for left/right
    - Failure: F -> None
    """
    if not reps_str or reps_str.strip() in ['NA', 'F', 'BF', 'GF', '']:
        return None, False, None

    reps_str = str(reps_str).strip()

    # Replace unicode fractions
    reps_str = reps_str.replace('⅛', '.125').replace('¼', '.25').replace('⅓', '.333')
    reps_str = reps_str.replace('½', '.5').replace('⅔', '.667').replace('¾', '.75')
    reps_str = reps_str.replace('⅚', '.833').replace('⅝', '.625').replace('⅜', '.375')

    has_extender = False

    # Handle single leg notation (left:right)
    if ':' in reps_str and '+' not in reps_str:
        parts = reps_str.split(':')
        try:
            left = float(parts[0]) if parts[0] else None
            right = float(parts[1]) if len(parts) > 1 and parts[1] else None
            return (left, right), False, 'single_leg'
        except ValueError:
            pass

    # Handle extender notation (13+3)
    if '+' in reps_str:
        parts = reps_str.split('+')
        try:
            base_reps = float(parts[0])
            extra_reps = float(parts[1]) if len(parts) > 1 and parts[1] else 0
            return base_reps + extra_reps, True, None
        except ValueError:
            pass

    # Try simple number
    try:
        return float(reps_str), False, None
    except ValueError:
        return None, False, None


def parse_set_string(set_str, exercise_standard, exercise_raw=""):
    """
    Parse a single set string like "90/8" or "3PPs+30/5.5" or "BW+25/6".
    Returns dict with weight_raw, weight_lbs, reps, has_extender, etc.
    """
    set_str = str(set_str).strip()

    # Handle "x2" or "x3" notation (same set repeated)
    repeat_match = re.search(r'x(\d+)$', set_str)
    repeat_count = int(repeat_match.group(1)) if repeat_match else 1
    if repeat_match:
        set_str = set_str[:repeat_match.start()]

    # Split by /
    parts = set_str.split('/')

    if len(parts) < 2:
        return None

    weight_raw = parts[0]
    reps_raw = parts[1]

    # Parse weight
    weight_lbs, is_bodyweight, weight_extender, _ = parse_weight_notation(weight_raw, exercise_standard, exercise_raw)

    # Parse reps
    reps, reps_extender, reps_type = parse_reps_notation(reps_raw)

    has_extender = weight_extender or reps_extender

    result = {
        'weight_raw': weight_raw,
        'weight_lbs': weight_lbs,
        'reps': reps,
        'has_extender': has_extender,
        'is_bodyweight': is_bodyweight,
        'reps_type': reps_type,
        'repeat_count': repeat_count,
    }

    return result


def parse_exercise_line(line):
    """
    Parse an exercise line like:
    "DB bench: 90/8, 80/12"
    "+Legend shoulder: 3PPs+30/5.5 3PPs+10/9"

    Also handles multi-exercise lines like:
    "Trcp pushdown: 170/6 OVH alt: 120/6 115/8"

    Returns (exercise_name, is_paused, sets_data) for first exercise
    """
    line = line.strip()
    if not line or ':' not in line:
        return None, False, []

    # Check for paused indicator
    is_paused = line.startswith('+')

    # Handle multi-exercise lines (look for second ":" that's followed by numbers with /)
    # Pattern: "Exercise1: sets OVH: sets" or "Exercise1: sets SA: sets"
    # Split on known secondary exercise patterns
    secondary_patterns = [
        r'\s+(OVH(?:\s+alt)?)\s*:', r'\s+(SA)\s*:', r'\s+(OVH\s+alt)\s*:',
        r'\s+(OVH)\s*:', r'\s+(SS)\s*:', r'\s+(RVS)\s*:'
    ]

    # For now, just take everything up to the second colon pattern if it exists
    # This is a simplification - we just keep the first exercise
    first_colon_pos = line.find(':')
    remaining = line[first_colon_pos+1:]

    # Check if there's another colon in the remaining part that looks like another exercise
    second_colon_pos = remaining.find(':')
    if second_colon_pos != -1:
        # Check if what's before the second colon looks like an exercise abbreviation
        potential_exercise = remaining[:second_colon_pos].strip().split()
        if potential_exercise:
            last_word = potential_exercise[-1]
            # If the last word before colon is a known abbreviation, truncate there
            known_abbreviations = ['OVH', 'SA', 'SS', 'RVS', 'alt', 'ALT']
            if last_word in known_abbreviations or (len(potential_exercise) > 1 and potential_exercise[-2] in known_abbreviations):
                # Find where this secondary exercise starts
                # Take only sets before this point
                for abbrev in known_abbreviations:
                    pattern = rf'\s+{abbrev}\s*:'
                    match = re.search(pattern, remaining, re.IGNORECASE)
                    if match:
                        remaining = remaining[:match.start()]
                        break

    # Split exercise name and sets
    parts = [line[:first_colon_pos].strip(), remaining.strip()]
    exercise_raw = parts[0]
    sets_str = parts[1] if len(parts) > 1 else ''

    # Skip if exercise_raw looks like a set notation (number/number pattern at start)
    if re.match(r'^\d+\.?\d*/', exercise_raw):
        return None, False, []

    # Get standardized name
    exercise_standard = EXERCISE_MAPPING.get(exercise_raw, exercise_raw)

    # Skip if mapping says to ignore this entry
    if exercise_standard is None:
        return None, False, []

    # Parse sets - split by comma or space (handling both formats)
    # Some entries use commas, some use spaces
    # But be careful with patterns like "80/12x2" or notes in parentheses
    sets_raw = re.split(r'[,\s]+', sets_str)
    sets_raw = [s.strip() for s in sets_raw if s.strip() and '/' in s]

    sets_data = []
    for set_str in sets_raw:
        # Skip if this looks like a secondary exercise name
        if ':' in set_str or set_str in ['OVH', 'SA', 'SS', 'alt']:
            continue
        parsed = parse_set_string(set_str, exercise_standard, exercise_raw)
        if parsed:
            # Handle repeat notation
            for _ in range(parsed['repeat_count']):
                sets_data.append({
                    'weight_raw': parsed['weight_raw'],
                    'weight_lbs': parsed['weight_lbs'],
                    'reps': parsed['reps'],
                    'has_extender': parsed['has_extender'],
                    'is_bodyweight': parsed['is_bodyweight'],
                    'reps_type': parsed['reps_type'],
                })

    return exercise_raw, is_paused, sets_data


def estimate_dates_for_month(year, month, num_sessions):
    """
    Estimate dates for sessions within a month based on ~3-4 workouts per week.
    """
    from calendar import monthrange

    _, days_in_month = monthrange(year, month)

    if num_sessions == 0:
        return []

    # Spread sessions roughly evenly across the month
    # Typical pattern: workout days with 1-2 rest days between

    dates = []
    if num_sessions == 1:
        dates = [15]  # Middle of month
    else:
        # Calculate spacing
        spacing = days_in_month / (num_sessions + 1)
        for i in range(num_sessions):
            day = int(spacing * (i + 1))
            day = min(day, days_in_month)
            day = max(day, 1)
            dates.append(day)

    return [datetime(year, month, d) for d in dates]


def is_workout_header(line):
    """
    Determine if a line is a workout type header.
    Headers are lines like: "Upper:", "Legs:", "Torso 1:", "Upper 2:", "Chest and arms:", etc.
    They don't contain "/" (weight/reps separator) and typically end with ":"
    """
    line = line.strip()

    # Skip empty lines
    if not line:
        return False

    # Skip lines with "/" - these are exercise lines with sets
    if '/' in line:
        return False

    # Skip lines that are clearly just text/notes
    if len(line) > 50:
        return False

    # Remove trailing colon for checking
    clean = line.rstrip(':').strip()

    # Known workout type patterns
    workout_patterns = [
        # Basic types (with optional number)
        r'^Upper\s*\d*$',
        r'^Lower\s*\d*$',
        r'^Legs\s*\d*$',
        r'^Back\s*\d*$',
        r'^Chest\s*\d*$',
        r'^Arms\s*\d*$',
        r'^Torso\s*\d*$',
        r'^Limbs\s*\d*$',
        # With explicit numbers (Upper 1, Upper 2, Lower 3, etc.)
        r'^Upper\s+\d+$',
        r'^Lower\s*\d+$',  # Handle "Lower2" without space
        r'^Legs\s+\d+$',
        r'^Back\s+\d+$',
        r'^Arms\s+\d+$',
        r'^Torso\s+\d+$',
        r'^Limbs\s+\d+$',
        # Combination types
        r'^Bro\s*[Dd]ay\s*\d*$',
        r'^Fun\s*[Dd]ay\s*\d*$',
        r'^Weakness\s*[Dd]ay\s*\d*$',
        r'^Chest\s+[Aa]nd\s+[Aa]rms\s*\d*$',
        r'^Chest\s+[Aa]nd\s+[Aa]rms\s*$',
        r'^Back\s+[Aa]nd\s+[Ss]houlders\s*\d*$',
        r'^Upper\s+[Bb]ack\s+[Aa]nd\s+[Ss]houlders\s*\d*$',
        # Special sessions
        r'^[Ii]solation[s]?\s*\d*$',
    ]

    for pattern in workout_patterns:
        if re.match(pattern, clean, re.IGNORECASE):
            return True

    return False


def parse_workout_file(filepath):
    """
    Parse a single workout file and return list of sessions.
    Each session is a dict with workout_type and exercises.
    """
    # Extract month and year from filename
    filename = os.path.basename(filepath)

    # Try to parse month/year from filename like "January 2024 gym sessions.txt"
    month_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', filename, re.IGNORECASE)

    if month_match:
        month_name = month_match.group(1)
        year = int(month_match.group(2))
        month_map = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12
        }
        month = month_map[month_name.lower()]
    else:
        # Default fallback
        year, month = 2024, 1

    # Read file
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        content = f.read()

    # Split into sessions by double newlines or workout type headers
    sessions = []
    current_session = None
    current_exercises = []

    lines = content.split('\n')

    # Track pending exercise name for multi-line format
    pending_exercise = None

    for line in lines:
        original_line = line
        line = line.strip()

        # Skip empty lines
        if not line:
            pending_exercise = None  # Reset on blank line
            continue

        # Skip month headers like "January 2024:" or "October 2024 gym sessions:"
        if re.match(r'^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}', line, re.IGNORECASE):
            continue

        # Check if this is a workout type header
        if is_workout_header(line):
            # Save previous session if exists
            if current_session and current_exercises:
                sessions.append({
                    'workout_type': current_session,
                    'exercises': current_exercises
                })

            # Start new session
            current_session = line.rstrip(':').strip()
            current_exercises = []
            pending_exercise = None

        elif ':' in line and '/' in line:
            # Exercise with sets on same line (standard format)
            exercise_raw, is_paused, sets_data = parse_exercise_line(line)
            if exercise_raw and sets_data:
                current_exercises.append({
                    'exercise_raw': exercise_raw,
                    'is_paused': is_paused,
                    'sets': sets_data
                })
            pending_exercise = None

        elif ':' in line and '/' not in line:
            # Exercise name only (multi-line format) - remember it for next line
            # But first check if it's not a workout header pattern we missed
            clean_line = line.rstrip(':').strip()
            if len(clean_line) < 40:  # Reasonable exercise name length
                pending_exercise = line
            else:
                pending_exercise = None

        elif '/' in line and ':' not in line and pending_exercise:
            # Sets only - combine with pending exercise name
            combined_line = pending_exercise + ' ' + line
            exercise_raw, is_paused, sets_data = parse_exercise_line(combined_line)
            if exercise_raw and sets_data:
                current_exercises.append({
                    'exercise_raw': exercise_raw,
                    'is_paused': is_paused,
                    'sets': sets_data
                })
            pending_exercise = None

        elif '/' in line:
            # Sets on their own line without a pending exercise - might be continuation
            # Try to add to last exercise if exists
            if current_exercises:
                # Parse as additional sets
                sets_raw = re.split(r'[,\s]+', line)
                sets_raw = [s.strip() for s in sets_raw if s.strip() and '/' in s]
                last_exercise = current_exercises[-1]
                exercise_raw = last_exercise['exercise_raw']
                exercise_standard = EXERCISE_MAPPING.get(exercise_raw, exercise_raw)
                for set_str in sets_raw:
                    parsed = parse_set_string(set_str, exercise_standard, exercise_raw)
                    if parsed:
                        for _ in range(parsed['repeat_count']):
                            last_exercise['sets'].append({
                                'weight_raw': parsed['weight_raw'],
                                'weight_lbs': parsed['weight_lbs'],
                                'reps': parsed['reps'],
                                'has_extender': parsed['has_extender'],
                                'is_bodyweight': parsed['is_bodyweight'],
                                'reps_type': parsed['reps_type'],
                            })

    # Don't forget last session
    if current_session and current_exercises:
        sessions.append({
            'workout_type': current_session,
            'exercises': current_exercises
        })

    # Estimate dates for sessions
    dates = estimate_dates_for_month(year, month, len(sessions))

    for i, session in enumerate(sessions):
        if i < len(dates):
            session['date'] = dates[i]
        else:
            # Fallback to end of month
            session['date'] = datetime(year, month, 28)
        session['year'] = year
        session['month'] = month

    return sessions


def get_day_of_week(date):
    """Return day name from datetime."""
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    return days[date.weekday()]


def build_training_sets_df(all_sessions):
    """
    Build the training_sets.csv dataframe from parsed sessions.
    """
    rows = []

    for session in all_sessions:
        date = session['date']
        workout_type = session['workout_type']
        day_of_week = get_day_of_week(date)

        for exercise in session['exercises']:
            exercise_raw = exercise['exercise_raw']
            exercise_standard = EXERCISE_MAPPING.get(exercise_raw, exercise_raw)
            is_paused = exercise['is_paused']

            for set_num, set_data in enumerate(exercise['sets'], 1):
                # Handle single leg reps (tuple)
                reps = set_data['reps']
                if isinstance(reps, tuple):
                    # For single leg, average the two sides
                    left, right = reps
                    if left is not None and right is not None:
                        reps = (left + right) / 2
                    elif left is not None:
                        reps = left
                    elif right is not None:
                        reps = right
                    else:
                        reps = None

                # Calculate volume
                weight_lbs = set_data['weight_lbs']
                if weight_lbs is not None and reps is not None:
                    volume = weight_lbs * reps
                else:
                    volume = None

                # Build notes
                notes = []
                if is_paused:
                    notes.append('paused')
                if set_data['is_bodyweight']:
                    notes.append('bodyweight')
                if set_data['reps_type'] == 'single_leg':
                    notes.append('single_leg')

                rows.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'day_of_week': day_of_week,
                    'workout_type': workout_type,
                    'exercise_raw': exercise_raw,
                    'exercise_standard': exercise_standard,
                    'weight_raw': set_data['weight_raw'],
                    'weight_lbs': weight_lbs,
                    'reps': reps,
                    'set_number': set_num,
                    'volume': volume,
                    'has_extender': set_data['has_extender'],
                    'machine_position': None,  # Will extract from exercise name if present
                    'is_synthetic': False,
                    'notes': ', '.join(notes) if notes else '',
                })

    return pd.DataFrame(rows)


def build_training_sessions_df(sets_df):
    """
    Build the training_sessions.csv dataframe from the sets dataframe.
    """
    # Group by date and workout_type
    sessions = sets_df.groupby(['date', 'workout_type']).agg({
        'day_of_week': 'first',
        'exercise_standard': lambda x: ','.join(x.unique()),
        'set_number': 'count',
        'volume': 'sum',
        'weight_lbs': 'mean',
        'reps': 'mean',
        'is_synthetic': 'any',
    }).reset_index()

    sessions.columns = [
        'date', 'workout_type', 'day_of_week', 'exercises_list',
        'num_sets', 'total_volume', 'avg_weight', 'avg_reps', 'is_synthetic'
    ]

    # Calculate num_exercises
    sessions['num_exercises'] = sessions['exercises_list'].apply(lambda x: len(x.split(',')))

    # Estimate session duration (sets * 3 minutes)
    sessions['session_duration_est'] = sessions['num_sets'] * 3

    # Calculate days since last session
    sessions = sessions.sort_values('date')
    sessions['date_dt'] = pd.to_datetime(sessions['date'])
    sessions['days_since_last'] = sessions['date_dt'].diff().dt.days.fillna(0).astype(int)
    sessions = sessions.drop('date_dt', axis=1)

    # Reorder columns
    sessions = sessions[[
        'date', 'day_of_week', 'workout_type', 'exercises_list',
        'num_exercises', 'num_sets', 'total_volume', 'avg_weight',
        'avg_reps', 'session_duration_est', 'days_since_last', 'is_synthetic'
    ]]

    return sessions


def extract_unique_exercises(all_sessions):
    """
    Extract all unique exercise names found in the data.
    """
    exercises = set()
    for session in all_sessions:
        for exercise in session['exercises']:
            exercises.add(exercise['exercise_raw'])
    return sorted(exercises)


def main():
    """Main entry point."""
    # Set console encoding for Windows
    import sys
    if sys.platform == 'win32':
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')

    print("=" * 60)
    print("STRENGTH TRAINING DATA PARSER")
    print("=" * 60)

    # Find all workout files
    workout_files = list(DATA_DIR.glob("*.txt"))
    print(f"\nFound {len(workout_files)} workout files")

    # Parse all files
    all_sessions = []
    for filepath in sorted(workout_files):
        print(f"  Parsing: {filepath.name}")
        sessions = parse_workout_file(filepath)
        all_sessions.extend(sessions)
        print(f"    -> {len(sessions)} sessions found")

    print(f"\nTotal sessions parsed: {len(all_sessions)}")

    # Extract unique exercises for review
    print("\n" + "=" * 60)
    print("UNIQUE EXERCISES FOUND")
    print("=" * 60)
    unique_exercises = extract_unique_exercises(all_sessions)
    unmapped = []
    for ex in unique_exercises:
        mapped = EXERCISE_MAPPING.get(ex, None)
        if mapped:
            print(f"  {ex} -> {mapped}")
        else:
            print(f"  {ex} -> [UNMAPPED]")
            unmapped.append(ex)

    if unmapped:
        print(f"\n{len(unmapped)} unmapped exercises found!")

    # Build dataframes
    print("\n" + "=" * 60)
    print("BUILDING DATAFRAMES")
    print("=" * 60)

    sets_df = build_training_sets_df(all_sessions)
    print(f"Training sets: {len(sets_df)} rows")

    sessions_df = build_training_sessions_df(sets_df)
    print(f"Training sessions: {len(sessions_df)} rows")

    # Export to CSV
    sets_output = OUTPUT_DIR / "training_sets.csv"
    sessions_output = OUTPUT_DIR / "training_sessions.csv"

    sets_df.to_csv(sets_output, index=False)
    sessions_df.to_csv(sessions_output, index=False)

    print(f"\nExported:")
    print(f"  {sets_output}")
    print(f"  {sessions_output}")

    # Summary statistics
    print("\n" + "=" * 60)
    print("SUMMARY STATISTICS")
    print("=" * 60)
    print(f"Date range: {sets_df['date'].min()} to {sets_df['date'].max()}")
    print(f"Total sessions: {len(sessions_df)} (real: {len(sessions_df[~sessions_df['is_synthetic']])}, synthetic: {len(sessions_df[sessions_df['is_synthetic']])})")
    print(f"Total sets: {len(sets_df)}")
    print(f"Unique exercises: {sets_df['exercise_standard'].nunique()}")
    print(f"Unique workout types: {sets_df['workout_type'].nunique()}")

    # Check for issues
    print("\n" + "=" * 60)
    print("VALIDATION CHECKS")
    print("=" * 60)

    # Check for negative values
    neg_weights = sets_df[sets_df['weight_lbs'] < 0] if 'weight_lbs' in sets_df.columns else pd.DataFrame()
    neg_reps = sets_df[sets_df['reps'] < 0] if 'reps' in sets_df.columns else pd.DataFrame()
    print(f"Negative weights: {len(neg_weights)}")
    print(f"Negative reps: {len(neg_reps)}")

    # Check for null values in key fields
    null_weights = sets_df['weight_lbs'].isna().sum()
    null_reps = sets_df['reps'].isna().sum()
    print(f"Null weights (FS/BW/NA): {null_weights}")
    print(f"Null reps: {null_reps}")

    print("\n" + "=" * 60)
    print("DONE!")
    print("=" * 60)


if __name__ == "__main__":
    main()
