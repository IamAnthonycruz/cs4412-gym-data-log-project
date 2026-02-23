# Discovery of Training Patterns and Exercise Progression Dynamics in Personal Strength Training Data

## Project Description

This project applies data mining techniques to analyze 2 years of personal strength training logs to discover patterns in exercise progression, workout structure, and performance. Using association rule mining, clustering, and anomaly detection, the aim is to uncover hidden relationships between exercises, identify natural training phases, and characterize sessions with unusual performance.

### Discovery Questions

1. **Exercise Association Patterns** - What exercises are frequently performed together within the same workout session, and what compound movement patterns emerge across different training splits?

2. **Training Phase Segmentation** - Can we identify distinct training phases based on volume, intensity, and exercise selection patterns without prior labeling?

3. **Performance Variation Patterns** - What characterizes sessions with unusually high or low performance compared to typical training sessions?

### Techniques

- **Association Rule Mining** (Apriori) - Discover exercise co-occurrence patterns
- **Clustering** (K-Means) - Identify natural training phase groupings
- **Anomaly Detection** (Isolation Forest, z-score) - Detect outlier sessions

## Dataset

**Name:** Personal Strength Training Log Dataset (PSTLD)

**Source:** Self-collected personal training logs (not publicly hosted)

**Size:**

- Time span: 2 years of training data
- Estimated 400–800 workout sessions
- Estimated 8,000–15,000 individual exercise sets
- 8–12 attributes per record

**Key Features:**

- Exercise name
- Weight/Load (plate notation and direct weight)
- Repetitions and sets
- Date and workout type
- Derived metrics: volume, intensity, frequency

## Project Structure

```
cs4412-project/
├── README.md
├── data/
│   └── .gitkeep
├── docs/
│   └── proposal.pdf
├── src/
│   └── .gitkeep
    |notebooks/
        └── .gitkeep
```

## Author

**Carlos Anthony Cruz**  
CS 4412: Data Mining  
Kennesaw State University  
Ccruz31@students.kennesaw.edu
