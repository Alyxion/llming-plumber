# LLM Blocks

> AI-powered text processing — chat, summarization, classification, extraction, translation, and more.

All LLM blocks support multiple providers: **OpenAI**, **Azure OpenAI**, **Anthropic**, **Google (Gemini)**, and **Mistral**. Each block uses a dedicated prompt template optimized for its task.

---

## Common Fields

Every LLM block includes these configuration fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **provider** | select | `openai` | LLM provider: `openai`, `azure_openai`, `anthropic`, `google`, `mistral` |
| **model** | str | — | Model name (e.g. `gpt-4o`, `claude-sonnet-4-20250514`, `gemini-2.0-flash`) |
| **temperature** | float | varies | Sampling temperature (0.0 = deterministic, 1.0 = creative) |
| **max_tokens** | int | varies | Maximum response tokens |

Provider credentials are read from environment variables (e.g. `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`).

---

## Chat & Q&A

### llm_chat

Send a message to an LLM and get a response. The most general-purpose LLM block.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **message** | str | — | The message to send |
| **system_prompt** | str | — | Optional system prompt to set behavior |
| *(common LLM fields)* | | | |

**Output:** `response` (str), `tokens_used` (int), `cost_usd` (float)

---

### llm_question_answerer

Answer a question based on provided context. Useful for building Q&A over documents.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **context** | str | — | Reference text to answer from |
| **question** | str | — | The question to answer |
| *(common LLM fields)* | | | |

**Output:** `answer` (str), `sources` (list), `confidence` (float)

---

## Text Processing

### llm_summarizer

Summarize text with configurable length and style.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **text** | str | — | Text to summarize |
| **length** | select | `medium` | `short`, `medium`, or `long` |
| **style** | select | `paragraph` | `bullet`, `paragraph`, or `highlights` |
| *(common LLM fields)* | | | |

**Output:** `summary` (str), `original_length` (int), `summary_length` (int), `ratio` (float)

---

### llm_rewriter

Rewrite text in a specified style or tone.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **text** | str | — | Text to rewrite |
| **style** | select | `formal` | `formal`, `casual`, `creative`, or `technical` |
| **tone** | str | — | Additional tone guidance |
| *(common LLM fields)* | | | |

**Output:** `rewritten` (str), `tokens_used` (int)

---

### llm_translator

Translate text between languages.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **text** | str | — | Text to translate |
| **source_language** | str | — | Source language (e.g. `English`, `de`) |
| **target_language** | str | — | Target language (e.g. `German`, `fr`) |
| **preserve_formatting** | bool | `true` | Preserve original formatting (bullets, paragraphs) |
| *(common LLM fields)* | | | |

**Output:** `translated` (str), `tokens_used` (int)

---

## Analysis

### llm_classifier

Classify text into one of several predefined categories.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **text** | str | — | Text to classify |
| **categories** | list[str] | — | List of category labels, e.g. `["complaint", "inquiry", "order"]` |
| *(common LLM fields)* | | | |

**Output:** `category` (str), `confidence` (float), `explanation` (str)

---

### llm_sentiment

Analyze the sentiment of text.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **text** | str | — | Text to analyze |
| *(common LLM fields)* | | | |

**Output:** `sentiment` (`positive`, `negative`, or `neutral`), `score` (float, -1.0 to 1.0), `explanation` (str)

---

### llm_entity_extractor

Extract named entities (people, organizations, locations, dates, etc.) from text.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **text** | str | — | Text to analyze |
| **entity_types** | list[str] | — | Types to extract, e.g. `["person", "organization", "date"]` |
| *(common LLM fields)* | | | |

**Output:** `entities` (list of `{text, type, start, end}` dicts), `count` (int), `tokens_used` (int)

---

### llm_data_extractor

Extract structured data from unstructured text according to a schema.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **text** | str | — | Text to extract from |
| **schema** | dict | — | JSON schema defining the expected output structure |
| *(common LLM fields)* | | | |

**Output:** `data` (dict matching the schema), `raw_response` (str), `tokens_used` (int)

Example schema:
```json
{
  "order_number": "string",
  "customer_name": "string",
  "total_amount": "number",
  "items": [{"name": "string", "quantity": "number"}]
}
```

---

## Common Pipelines

**Email triage:**
```
[Email Trigger] → [LLM Classifier (categories: complaint, inquiry, order)] → [Conditional] → ...
```

**Document summarization:**
```
[PDF Reader] → [Split Text (chunks)] → [LLM Summarizer] → [Collect] → [Text Template]
```

**Multilingual content:**
```
[Web Crawler] → [LLM Translator (target: German)] → [Excel Builder]
```
