# Fabric notebook source, do not edit
# Notebook: 07_Data_Agent_Evaluation

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {}
# META   }
# META }

# MARKDOWN ********************

# # Data Agent Evaluation — LLM-as-Judge
# 
# This notebook evaluates your Data Agent's accuracy by:
# 1. Executing DAX queries against the semantic model to generate **ground truth**
# 2. Querying the Data Agent via the **Fabric Data Agent SDK**
# 3. Using an **LLM-as-judge** to compare agent responses to ground truth
# 4. **Calibrating the judge** to measure its own reliability
# 5. Producing a **calibration-adjusted final score**

# CELL ********************

# Cell 1: Install Dependencies
%pip install -U fabric-data-agent-sdk openai -q

# CELL ********************

# Cell 2: Imports & Configuration
import sempy.fabric as fabric
import pandas as pd
import time

# Your workspace and semantic model — update if names differ
WORKSPACE_ID = fabric.resolve_workspace_id()
SEMANTIC_MODEL = "HealthcareLakehouse-SemanticModel"  # From Module 3
DATA_AGENT_NAME = "HealthFirst Clinical Analyst"       # From Module 7

print(f"Workspace ID: {WORKSPACE_ID}")
print(f"Semantic Model: {SEMANTIC_MODEL}")
print(f"Data Agent: {DATA_AGENT_NAME}")

# CELL ********************

# Cell 3: Define DAX Queries (Ground Truth)
# Each entry has a natural-language question and the DAX query
# that produces the definitive correct answer.

dax_queries = [
    {
        "question": "What is the 30-day readmission rate?",
        "dax": """
EVALUATE
ROW(
    "Readmission Rate", 
    DIVIDE(
        COUNTROWS(FILTER('gold_readmissions', 'gold_readmissions'[is_readmission] = TRUE())),
        COUNTROWS('gold_readmissions')
    )
)
"""
    },
    {
        "question": "Which facility has the highest average length of stay for inpatient encounters?",
        "dax": """
EVALUATE
TOPN(
    1,
    SUMMARIZECOLUMNS(
        'gold_encounter_summary'[facility_name],
        FILTER(ALL('gold_encounter_summary'), 'gold_encounter_summary'[encounter_type] = "Inpatient"),
        "Avg LOS", AVERAGE('gold_encounter_summary'[length_of_stay_days])
    ),
    [Avg LOS], DESC
)
"""
    },
    {
        "question": "How many patients have diabetes?",
        "dax": """
EVALUATE
ROW(
    "Diabetes Patients",
    COUNTROWS(
        FILTER(
            'gold_chronic_conditions',
            'gold_chronic_conditions'[has_diabetes] = TRUE()
        )
    )
)
"""
    },
    {
        "question": "What is the claim denial rate by insurance type?",
        "dax": """
EVALUATE
SUMMARIZECOLUMNS(
    'gold_encounter_summary'[insurance_type],
    "Denial Rate", 
    DIVIDE(
        CALCULATE(COUNTROWS('gold_encounter_summary'), 'gold_encounter_summary'[claim_status] = "Denied"),
        COUNTROWS('gold_encounter_summary')
    )
)
ORDER BY [Denial Rate] DESC
"""
    },
    {
        "question": "Who are the top 3 patients by total number of encounters?",
        "dax": """
EVALUATE
TOPN(
    3,
    SUMMARIZECOLUMNS(
        'silver_patients'[patient_id],
        'silver_patients'[first_name],
        'silver_patients'[last_name],
        "Total Encounters", COUNTROWS('gold_encounter_summary')
    ),
    [Total Encounters], DESC
)
ORDER BY [Total Encounters] DESC
"""
    }
]

df_questions = pd.DataFrame(dax_queries)
print(f"✅ {len(dax_queries)} evaluation questions defined")
display(df_questions[["question"]])

# CELL ********************

# Cell 4: Execute DAX Queries for Ground Truth

def format_dax_result(df_result):
    """Convert a DAX result DataFrame into a human-readable string."""
    if df_result is None or df_result.empty:
        return "No results"
    
    # Single-value result
    if len(df_result) == 1 and len(df_result.columns) == 1:
        return str(df_result.iloc[0, 0])
    
    # Single-row, multi-column result
    if len(df_result) == 1:
        parts = [f"{col}: {df_result.iloc[0][col]}" for col in df_result.columns]
        return ", ".join(parts)
    
    # Multi-row result: format as numbered list
    lines = []
    for idx, (_, row) in enumerate(df_result.iterrows(), 1):
        parts = [f"{col}: {row[col]}" for col in df_result.columns]
        lines.append(f"{idx}. " + ", ".join(parts))
    return "\n".join(lines)


# Execute each DAX query
print("Executing DAX queries against semantic model...")
ground_truth = []

for i, row in df_questions.iterrows():
    try:
        result = fabric.evaluate_dax(SEMANTIC_MODEL, row["dax"])
        answer = format_dax_result(result)
        ground_truth.append(answer)
        print(f"  ✅ Q{i+1}: {row['question'][:50]}...")
        print(f"     → {answer[:100]}")
    except Exception as e:
        ground_truth.append(f"ERROR: {str(e)}")
        print(f"  ❌ Q{i+1}: {str(e)[:80]}")

# Build evaluation DataFrame
eval_df = pd.DataFrame({
    "question": df_questions["question"].tolist(),
    "expected_answer": ground_truth
})

print(f"\n{'='*60}")
print(f"Ground truth generated for {len(eval_df)} questions")
errors = sum(1 for a in ground_truth if a.startswith("ERROR"))
if errors > 0:
    print(f"⚠️ {errors} queries failed — fix DAX before proceeding")
print(f"{'='*60}")
display(eval_df)

# CELL ********************

# Cell 5: Define LLM-as-Judge Critic Prompts

critic_prompt = """You are an impartial judge evaluating whether a data agent's answer is correct, using the ground truth as the reference.

Rules (apply in order):
1. Numerical accuracy: values must match within 1% relative tolerance. Example: 1,234,567 vs 1,234,000 is YES; 1,234,567 vs 2,500,000 is NO.
2. Entity completeness: every entity in the ground truth (patient IDs, facility names, diagnosis names) must appear in the answer. A missing entity is NO even if everything else matches.
3. Rank order: for ranked or ordered results, rank positions must match. Correct entities in the wrong rank is NO.
4. Semantic equivalence: ignore formatting differences (table vs list, markdown vs plain text, currency symbols, capitalisation, percentage vs decimal).
5. Statistical measures: mean, median, mode, sum, and count are not interchangeable. A wrong measure is NO even if the value matches.
6. Extra information: an answer that contains all ground truth data plus additional non-contradictory context is still YES.
7. Refusals: if the answer says it cannot find data but the ground truth contains data, that is NO.
8. Time periods: if the answer uses a different time period than the ground truth, that is NO.

Respond with exactly one word: Yes or No.

Query: {query}

Ground Truth: {expected_answer}
"""

# Calibration version — receives {actual_answer} explicitly
calibration_judge_prompt = """You are an impartial judge evaluating whether a data agent's answer is correct, using the ground truth as the reference.

Rules (apply in order):
1. Numerical accuracy: values must match within 1% relative tolerance. Example: 1,234,567 vs 1,234,000 is YES; 1,234,567 vs 2,500,000 is NO.
2. Entity completeness: every entity in the ground truth (patient IDs, facility names, diagnosis names) must appear in the answer. A missing entity is NO even if everything else matches.
3. Rank order: for ranked or ordered results, rank positions must match. Correct entities in the wrong rank is NO.
4. Semantic equivalence: ignore formatting differences (table vs list, markdown vs plain text, currency symbols, capitalisation, percentage vs decimal).
5. Statistical measures: mean, median, mode, sum, and count are not interchangeable. A wrong measure is NO even if the value matches.
6. Extra information: an answer that contains all ground truth data plus additional non-contradictory context is still YES.
7. Refusals: if the answer says it cannot find data but the ground truth contains data, that is NO.
8. Time periods: if the answer uses a different time period than the ground truth, that is NO.

Respond with exactly one word: Yes or No.

Query: {query}

Ground Truth: {expected_answer}

Actual Answer: {actual_answer}
"""

print("✅ Critic prompts defined")
print(f"   critic_prompt: {len(critic_prompt)} chars (8 rules)")
print(f"   calibration_judge_prompt: {len(calibration_judge_prompt)} chars")

# CELL ********************

# Cell 6: Judge Calibration — Meta-Evaluation

from synapse.ml.fabric.credentials import get_openai_httpx_sync_client
import openai

judge_client = openai.AzureOpenAI(
    http_client=get_openai_httpx_sync_client(),
    api_version="2025-04-01-preview",
)

# Healthcare-specific calibration cases
calibration_cases = [
    # ── TRUE POSITIVES (judge must say YES) ──────────────────────────────
    {
        "query": "How many patients have diabetes?",
        "expected": "42",
        "actual": "There are 42 patients diagnosed with diabetes in the system.",
        "expected_verdict": "yes",
        "note": "Exact number embedded in prose"
    },
    {
        "query": "What is the readmission rate?",
        "expected": "0.15",
        "actual": "The 30-day readmission rate is 15%.",
        "expected_verdict": "yes",
        "note": "0.15 == 15% — format equivalence"
    },
    {
        "query": "What is the average length of stay?",
        "expected": "4.567 days",
        "actual": "Average length of stay is 4.57 days.",
        "expected_verdict": "yes",
        "note": "Rounded to 2 decimal places, within 1%"
    },
    {
        "query": "Top 3 facilities by readmission rate?",
        "expected": "1. Metro General: 22%\n2. City Hospital: 18%\n3. Valley Medical: 15%",
        "actual": "The top 3 facilities are Metro General at 22%, City Hospital at 18%, and Valley Medical at 15%.",
        "expected_verdict": "yes",
        "note": "Table-to-prose, same rank order preserved"
    },
    {
        "query": "What is the claim denial rate?",
        "expected": "12%",
        "actual": "The overall claim denial rate is 12%. This is below the industry average of 15%.",
        "expected_verdict": "yes",
        "note": "Extra benchmark context should not penalise"
    },
    {
        "query": "What is the total billed amount?",
        "expected": "Total billed amount is $2,400,000.",
        "actual": "Total billing is $2.4M.",
        "expected_verdict": "yes",
        "note": "$2.4M == $2,400,000 within tolerance"
    },
    {
        "query": "What is the ED revisit rate?",
        "expected": "8%",
        "actual": "The ED revisit rate is 0.08.",
        "expected_verdict": "yes",
        "note": "Fraction vs percentage representation"
    },

    # ── TRUE NEGATIVES (judge must say NO) ───────────────────────────────
    {
        "query": "How many inpatient encounters were there?",
        "expected": "150",
        "actual": "There were 250 inpatient encounters.",
        "expected_verdict": "no",
        "note": "250 vs 150 — far outside 1%"
    },
    {
        "query": "Which facility has the highest readmission rate?",
        "expected": "Metro General",
        "actual": "The facility with the highest readmission rate is Valley Medical Center.",
        "expected_verdict": "no",
        "note": "Wrong facility entirely"
    },
    {
        "query": "What is the total cost of care?",
        "expected": "1,234,567",
        "actual": "Total cost of care was $2,500,000.",
        "expected_verdict": "no",
        "note": "Different number, different magnitude"
    },
    {
        "query": "Top 3 patients by encounter count?",
        "expected": "1. patient P001: 15\n2. patient P002: 12\n3. patient P003: 10",
        "actual": "The top 3 patients are P001 with 15, P004 with 12, and P003 with 10.",
        "expected_verdict": "no",
        "note": "P002 replaced by P004 — one wrong entity"
    },
    {
        "query": "Top 3 diagnoses by frequency?",
        "expected": "1. Hypertension\n2. Diabetes\n3. COPD",
        "actual": "Diabetes (1st), Hypertension (2nd), COPD (3rd).",
        "expected_verdict": "no",
        "note": "All entities present but rank order inverted"
    },
    {
        "query": "List all facilities with readmission rate above 20%.",
        "expected": "Metro General (24%), City Hospital (22%), Valley Medical (21%)",
        "actual": "Metro General with 24% and City Hospital with 22% have rates above 20%.",
        "expected_verdict": "no",
        "note": "Valley Medical (21%) silently dropped"
    },
    {
        "query": "How many encounters were flagged as readmissions in January?",
        "expected": "317 encounters were flagged as readmissions in January.",
        "actual": "I was unable to find data on readmission flags for January.",
        "expected_verdict": "no",
        "note": "Refusal when ground truth has a concrete answer"
    },
    {
        "query": "What was the readmission rate in Q3?",
        "expected": "Readmission rate in Q3 was 14.2%.",
        "actual": "The readmission rate was 14.2% in Q4.",
        "expected_verdict": "no",
        "note": "Correct value, wrong quarter"
    },
    {
        "query": "What is the median length of stay?",
        "expected": "3 days",
        "actual": "The average length of stay is 3 days.",
        "expected_verdict": "no",
        "note": "Mean ≠ median even if value is identical"
    },
]

# ─── Run calibration ─────────────────────────────────────────────────────
print(f"Running judge calibration with {len(calibration_cases)} test cases...")
judge_results = []

for case in calibration_cases:
    filled_prompt = calibration_judge_prompt.format(
        query=case["query"],
        expected_answer=case["expected"],
        actual_answer=case["actual"]
    )
    response = judge_client.chat.completions.create(
        model="gpt-4.1",
        messages=[{"role": "user", "content": filled_prompt}],
        temperature=0,
    )
    raw_verdict = response.choices[0].message.content.strip().lower()

    if raw_verdict.startswith("yes"):
        judge_verdict = "yes"
    elif raw_verdict.startswith("no"):
        judge_verdict = "no"
    else:
        judge_verdict = f"unexpected: {raw_verdict}"

    is_correct = judge_verdict == case["expected_verdict"]
    judge_results.append({
        "query": case["query"][:50],
        "note": case.get("note", ""),
        "expected_verdict": case["expected_verdict"],
        "judge_verdict": judge_verdict,
        "correct": is_correct
    })

judge_cal_df = pd.DataFrame(judge_results)
display(judge_cal_df)

# ─── Calibration metrics ─────────────────────────────────────────────────
tp = judge_cal_df[(judge_cal_df["expected_verdict"] == "yes") & (judge_cal_df["judge_verdict"] == "yes")].shape[0]
fp = judge_cal_df[(judge_cal_df["expected_verdict"] == "no")  & (judge_cal_df["judge_verdict"] == "yes")].shape[0]
fn = judge_cal_df[(judge_cal_df["expected_verdict"] == "yes") & (judge_cal_df["judge_verdict"] == "no")].shape[0]
tn = judge_cal_df[(judge_cal_df["expected_verdict"] == "no")  & (judge_cal_df["judge_verdict"] == "no")].shape[0]

judge_precision = tp / (tp + fp) if (tp + fp) > 0 else 0
judge_recall    = tp / (tp + fn) if (tp + fn) > 0 else 0
judge_f1        = 2 * judge_precision * judge_recall / (judge_precision + judge_recall) if (judge_precision + judge_recall) > 0 else 0
judge_accuracy  = judge_cal_df["correct"].sum() / len(judge_cal_df)
judge_tpr       = judge_recall
judge_fpr       = fp / (fp + tn) if (fp + tn) > 0 else 0

print(f"\n{'='*50}")
print(f"  JUDGE CALIBRATION RESULTS")
print(f"{'='*50}")
print(f"  Total test cases:    {len(judge_cal_df)}")
print(f"  Correct verdicts:    {judge_cal_df['correct'].sum()}/{len(judge_cal_df)} ({judge_accuracy*100:.0f}%)")
print(f"  Precision:           {judge_precision:.2f}")
print(f"  Recall:              {judge_recall:.2f}")
print(f"  F1 Score:            {judge_f1:.2f}")
print(f"  True Positive Rate:  {judge_tpr:.2f}")
print(f"  False Positive Rate: {judge_fpr:.2f}")
print(f"{'='*50}")

failures = judge_cal_df[~judge_cal_df["correct"]]
if not failures.empty:
    print(f"\nFailed cases ({len(failures)}):")
    for _, row in failures.iterrows():
        print(f"  [{row['expected_verdict']} → {row['judge_verdict']}] {row['query'][:60]} | {row['note']}")

if judge_accuracy < 0.75:
    print("\n⚠️ WARNING: Judge accuracy below 75%. Refine the critic prompt before proceeding.")
elif judge_accuracy < 0.90:
    print("\n⚡ Judge accuracy is moderate. Interpret results with caution.")
else:
    print("\n✅ Judge accuracy is high. Critic prompt is reliable.")

# CELL ********************

# Cell 7: Run Data Agent Evaluation via SDK
from fabric.dataagent.evaluation import evaluate_data_agent

TABLE_NAME = "data_agent_eval_results"
DATA_AGENT_STAGE = "production"

print(f"Starting evaluation of '{DATA_AGENT_NAME}'...")
print(f"  Questions: {len(eval_df)}")
print(f"  Results table: {TABLE_NAME}")
print(f"  Stage: {DATA_AGENT_STAGE}")
print()

evaluation_id = evaluate_data_agent(
    eval_df,
    DATA_AGENT_NAME,
    workspace_name=None,
    critic_prompt=critic_prompt,
    table_name=TABLE_NAME,
    data_agent_stage=DATA_AGENT_STAGE
)

print(f"\n✅ Evaluation complete!")
print(f"   Evaluation ID: {evaluation_id}")
print(f"   Results stored in: {TABLE_NAME}")

# CELL ********************

# Cell 8: Evaluation Summary
from fabric.dataagent.evaluation import get_evaluation_summary

eval_summary = get_evaluation_summary(TABLE_NAME)
print("📊 Evaluation Summary:")
display(eval_summary)

# CELL ********************

# Cell 9: Detailed Evaluation Results
from fabric.dataagent.evaluation import get_evaluation_details

eval_details = get_evaluation_details(
    evaluation_id,
    TABLE_NAME,
    get_all_rows=True,
    verbose=True
)
display(eval_details)

# CELL ********************

# Cell 10: Calibration-Adjusted Final Score

# Determine the result column from SDK output
result_col = None
for candidate in ["evaluation_judgement", "evaluation_result", "eval_result", "result", "is_correct", "pass"]:
    if candidate in eval_details.columns:
        result_col = candidate
        break

if result_col is None:
    for col in eval_details.columns:
        if "result" in col.lower() or "judg" in col.lower():
            result_col = col
            break

if result_col is None:
    raise RuntimeError(
        f"Cannot find evaluation result column. Available: {eval_details.columns.tolist()}"
    )

print(f"Using result column: '{result_col}'")

# Count passed
total = len(eval_details)
result_vals = eval_details[result_col]
if result_vals.dtype == bool:
    passed = result_vals.sum()
else:
    passed = result_vals.astype(str).str.strip().str.lower().isin(["true", "yes", "1"]).sum()
failed = total - passed
raw_accuracy = (passed / total) * 100 if total > 0 else 0

# Observed pass rate
observed_pass_rate = passed / total if total > 0 else 0

# Debiased estimate using judge calibration error rates
if (judge_tpr - judge_fpr) > 0:
    adjusted_pass_rate = (observed_pass_rate - judge_fpr) / (judge_tpr - judge_fpr)
    adjusted_pass_rate = max(0.0, min(1.0, adjusted_pass_rate))
else:
    adjusted_pass_rate = observed_pass_rate

# Confidence bounds based on judge accuracy
lower_bound = max(0, adjusted_pass_rate - (1 - judge_accuracy))
upper_bound = min(1, adjusted_pass_rate + (1 - judge_accuracy))

print(f"\n{'='*60}")
print(f"  FINAL EVALUATION REPORT")
print(f"{'='*60}")
print(f"  Total Questions:           {total}")
print(f"  Raw Passed (judge says):   {passed} ✅  /  {failed} ❌")
print(f"  Raw Accuracy:              {raw_accuracy:.1f}%")
print(f"{'─'*60}")
print(f"  Judge Calibration:")
print(f"    Judge Accuracy:          {judge_accuracy*100:.0f}%")
print(f"    Judge Precision:         {judge_precision:.2f}")
print(f"    Judge Recall:            {judge_recall:.2f}")
print(f"    Judge F1:                {judge_f1:.2f}")
print(f"    False Positive Rate:     {judge_fpr:.2f}")
print(f"    False Negative Rate:     {1 - judge_tpr:.2f}")
print(f"{'─'*60}")
print(f"  Calibration-Adjusted Score:")
print(f"    Adjusted Accuracy:       {adjusted_pass_rate*100:.1f}%")
print(f"    Confidence Range:        [{lower_bound*100:.1f}% — {upper_bound*100:.1f}%]")
print(f"{'='*60}")

if judge_accuracy >= 0.9:
    print("\n✅ High judge reliability — adjusted score is trustworthy.")
elif judge_accuracy >= 0.75:
    print("\n⚡ Moderate judge reliability — interpret with caution, see confidence range.")
else:
    print("\n⚠️ Low judge reliability — raw score may be unreliable. Refine critic prompt.")
