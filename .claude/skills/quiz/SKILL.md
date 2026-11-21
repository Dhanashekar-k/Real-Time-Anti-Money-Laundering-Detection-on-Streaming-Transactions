---
name: quiz
description: Interview the user on the code of a stream-aml milestone as a demanding senior engineer would, one question at a time, and log weak spots. Use when the user types /quiz Mxx.
argument-hint: "[M00-M11]"
disable-model-invocation: true
---

Quiz the user on milestone $ARGUMENTS.

Preparation (silent, don't show): read the milestone file in `manual/milestones/` whose name starts with "$ARGUMENTS" (especially its "Interview questions" section), the human-owned files it touches, and the results JSON it produced.

Rules:
- Ask **one question at a time** and wait for the answer. 8 questions total.
- Mix: 3 "explain this code" questions that point at a specific file:line, 3 "what happens if…" failure questions (crash, late event, duplicate, schema change, model swap), 2 "why this design over X" questions.
- Questions must be answerable from this repo, not trivia.
- After each answer: grade it **correct / partially correct / wrong**, state precisely what was missing or wrong, and cite the code line or result file that shows the right answer. No flattery.
- If the user says "skip", mark it wrong and move on.

At the end:
- Score out of 8 and the 2–3 weakest topics.
- Append to `docs/quiz_log.md`: date, milestone, score, weak topics, and one sentence per wrong answer stating the correct fact. (This is the only file you write.)
- Suggest one concrete exercise for the weakest topic (e.g. "re-implement onTimer from memory, then run GoldenVectorTest").
