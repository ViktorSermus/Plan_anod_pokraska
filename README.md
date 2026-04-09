# Plan_anod_pokraska

Веб-приложение (Streamlit) для производственного плана по аноду и покраске:

- импорт **заявок (ZNOM)** — **несколько Excel-файлов** через интерфейс;
- импорт **реестра готовности** — **один** Excel-файл (необязательно: без реестра часть полей будет пустой);
- хранение в **PostgreSQL** (рекомендуется проект [Supabase](https://supabase.com));
- вход пользователей через **Supabase Auth** (email/пароль и при настройке — Google);
- **журнал изменений** (`audit_log`): ручные правки полей «Вывезено», коррекция, примечание; а также запись по факту импорта из файлов.

Локальные папки `ZNOM` / `Reestr_ZNOM` и SQLite в текущей версии UI **не используются** (модуль `app/config.py` оставлен для совместимости со старыми сценариями).

## Требования

- Python 3.11+
- Проект Supabase: база PostgreSQL, включённый **Authentication**, при необходимости провайдер **Google**
- Таблицы `master_data` и `audit_log` (можно создать SQL из кабинета Supabase, см. раздел ниже)

## Секреты (обязательно)

Скопируйте [`.streamlit/secrets.toml.example`](.streamlit/secrets.toml.example) в **`.streamlit/secrets.toml`** и заполните:

| Ключ | Назначение |
|------|------------|
| `DATABASE_URL` | Строка подключения Postgres (из Supabase → Connect / Database) |
| `SUPABASE_URL` | URL проекта (`https://xxx.supabase.co`) |
| `SUPABASE_ANON_KEY` | anon / public API key |
| `APP_BASE_URL` | Публичный URL приложения: локально `http://localhost:8501`, в облаке — URL Streamlit Cloud |

Для **Google OAuth** в Supabase укажите тот же `APP_BASE_URL` в списке redirect URLs.

На **Streamlit Community Cloud** те же переменные задаются в **Secrets** приложения (формат TOML, как в примере).

## SQL для Supabase (если таблиц ещё нет)

Выполните в SQL Editor:

```sql
CREATE TABLE IF NOT EXISTS master_data (
    business_key TEXT PRIMARY KEY,
    date_request TEXT,
    request_no TEXT,
    item_name TEXT,
    service TEXT,
    author TEXT,
    client TEXT,
    qty_mp DOUBLE PRECISION,
    qty_bars DOUBLE PRECISION,
    moved_mp DOUBLE PRECISION,
    reserved_mp DOUBLE PRECISION,
    processed_mp DOUBLE PRECISION,
    processed_bars DOUBLE PRECISION,
    exported DOUBLE PRECISION,
    correction DOUBLE PRECISION,
    note TEXT,
    remaining DOUBLE PRECISION,
    is_active INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actor_user_id TEXT,
    actor_email TEXT,
    action TEXT NOT NULL,
    business_key TEXT,
    field_name TEXT,
    old_value TEXT,
    new_value TEXT,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON audit_log (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_business_key ON audit_log (business_key);
```

## Запуск локально

1. `pip install -r requirements.txt`
2. Настроить `.streamlit/secrets.toml`
3. В Supabase Auth создать пользователя (или включить регистрацию)
4. `streamlit run streamlit_app.py`

Скрипт `start_app.bat` по-прежнему создаёт venv и ставит зависимости; без `secrets.toml` приложение покажет ошибку о недостающих переменных.

## Как работает сохранение ручных полей

- Для строк считается `business_key` (хэш от № заявки, даты и наименования).
- При импорте из Excel значения «Вывезено», коррекция и примечание подтягиваются из базы по ключу.
- Исчезнувшие из импорта строки помечаются `is_active = 0` (архив), если включено архивирование (`ARCHIVE_MISSING_AS_INACTIVE` в secrets, по умолчанию да).

## Примечания

- Исходные Excel-файлы не изменяются, чтение только из памяти при загрузке.
- Проблемные файлы пропускаются с предупреждением в интерфейсе.
- Выгрузка журнала `audit_log` в файл на экране пока не выводится (данные накапливаются в БД).
