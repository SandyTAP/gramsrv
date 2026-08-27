# Админ-панель: все пути

Документация по всем путям встроенной админ-панели `telesrv` — адреса страниц
(SPA) и API-маршруты, которые за ними стоят. Сервер панели живёт в
`cmd/telesrv-admin`, маршруты объявлены в функции `(*server).routes()`
(`server.go:50`).

Панель — это обычный HTTP-сервер: он отдаёт собранный фронтенд (`/`) и JSON-API
под префиксом `/api/`. Все API-пути требуют сессию (cookie), а все
изменяющие запросы — ещё и CSRF-токен. Многие маршруты дополнительно проверяют
права оператора (`permission`).

## Аутентификация и сессия

| Метод | Путь | Описание |
| --- | --- | --- |
| POST | `/api/login` | Вход. Принимает `{"secret": "..."}`, проверяет секрет из конфига, выдаёт подписанную cookie-сессию и cookie CSRF. Единственный изменяющий маршрут без CSRF-токена (ещё нет сессии); проверяется только Origin. |
| POST | `/api/logout` | Выход. Уничтожает сессию и cookie CSRF. |
| GET | `/api/session` | Возвращает текущую сессию: имя оператора (`actor`) и список выданных прав (`permissions`). Панель вызывает его при старте. |

Сессия живёт в подписанной cookie (TTL по умолчанию 12 часов), серверного
хранилища сессий нет. Все запросы, кроме `GET/HEAD/OPTIONS` и `/api/login`,
обязаны предъявить:
- cookie `telesrv_admin_csrf`;
- заголовок `X-CSRF-Token` с тем же значением;
- Origin, совпадающий с хостом (если Origin присутствует).

Нарушение любого из трёх условий — `403 Forbidden`.

## Права и их проверка

Панель различает обычную авторизацию (`requireAuthAPI`) и проверку права
(`requirePermission`). Имена прав совпадают со строками в
`TELESRV_ADMIN_UI_PERMISSIONS`:

| Право | Что даёт |
| --- | --- |
| `*` | Все права (wildcard). |
| `premium.manage` | Управление каталогом Premium-планов, начисление Premium. |
| `bots.token.read` | Экспорт токена бота (`/api/actions/export-bot-token`). |
| `verification.review` | Чтение и решение очереди официальной верификации. |
| `verification.revoke` | Снятие официального бейджа (в дополнение к `verification.review`). |
| `botverification.review` | Чтение и решение очереди сторонней (ботовой) верификации. |
| `botverification.manage` | Назначение верификаторов, редактирование каталога иконок, снятие метки сторонней верификации. |

Права официальной и сторонней верификации намеренно независимы. Список прав
сессии возвращается в `/api/session`, чтобы интерфейс скрывал недоступные
разделы.

## Страницы интерфейса (SPA)

SPA-роуты декларативны: любое имя раздела отдаётся сервером как `/` (index.html),
а сам фронтенд решает, что рендерить (`web/src/pages/Routes.tsx`).

| Путь | Страница |
| --- | --- |
| `/` | Панель управления (счётчики, статистика хранилища, ссылки на разделы). |
| `/accounts` | Список аккаунтов (с поиском). |
| `/accounts/{id}` | Карточка аккаунта: профиль, действия. |
| `/channels` | Список супергрупп и каналов. |
| `/channels/{id}` | Карточка канала. |
| `/bots` | Список ботов. |
| `/bots/{id}` | Карточка бота. |
| `/broadcasts` | Рассылки. |
| `/monetization`, `/premium` | Звёзды и Premium: планы, начисление. Требует `premium.manage`. |
| `/moderation` | Жалобы и модерация: список кейсов. |
| `/moderation/{id}` | Детали кейса модерации. |
| `/emoji` | Каталог emoji-наборов. |
| `/stickers` | Каталог стикерпаков. |
| `/gif-catalog` | Каталог GIF. |
| `/messages`, `/messages/private` | Аудит личных сообщений. |
| `/messages/detail`, `/messages/private/detail` | Детали личного сообщения (`?owner_user_id=&msg_id=`). |
| `/messages/groups` | Аудит сообщений групп/каналов. |
| `/messages/groups/detail` | Детали сообщения в группе (`?channel_id=&msg_id=`). |
| `/gifts` | Звёздные подарки: каталог, коллекционки. |
| `/give-gifts` | Выдача подарков. |
| `/collectible-usernames` | Коллекционные юзернеймы. |
| `/collectible-usernames/{id}` | Карточка коллекционного юзернейма. |
| `/collectible-phones` | Анонимные номера. |
| `/account-ratings` | Рейтинг аккаунтов. |
| `/account-ratings/{user_id}` | Карточка рейтинга аккаунта. |
| `/storage` | Объектное хранилище: статистика. |
| `/verification` | Официальная верификация: очередь заявок. Требует `verification.review`. |
| `/verification/{id}` | Детали заявки на официальную верификацию. Требует `verification.review`. |
| `/bot-verification` | Сторонняя верификация: верификаторы, иконки, метки, очередь. Требует `botverification.review`. |
| `/bot-verification/{id}` | Детали запроса сторонней верификации. Требует `botverification.review`. |

Несуществующий API-путь возвращает `404 {"error":"api route not found"}`; любой
неизвестный путь фронтенда отдаётся как `/`.

## API: дашборд и хранилище

| Метод | Путь | Описание |
| --- | --- | --- |
| GET | `/api/dashboard` | Сводка: счётчики (`counts`) и статистика хранилища (`storage`), при наличии — `host`. |
| GET | `/api/storage/stats` | Статистика объектного хранилища. |

## API: аккаунты

| Метод | Путь | Описание |
| --- | --- | --- |
| GET | `/api/accounts` | Список/поиск аккаунтов. Параметры: `q` (поиск), `before_id`, `before_active_us`, `limit` (пагинация). Ответ: `rows`, `has_more`, `next_before_id`, `next_before_active_us`, `listing`. |
| GET | `/api/accounts/{id}` | Карточка аккаунта: профиль, флаги, статистика. |
| GET | `/api/accounts/{id}/avatar` | Аватар аккаунта (файл). |
| GET | `/api/account-ratings` | Список рейтингов аккаунтов. |
| GET | `/api/account-ratings/{user_id}` | Рейтинг конкретного аккаунта. |

## API: каналы и супергруппы

| Метод | Путь | Описание |
| --- | --- | --- |
| GET | `/api/channels` | Список каналов/супергрупп. |
| GET | `/api/channels/{id}` | Карточка канала. |
| GET | `/api/channels/{id}/avatar` | Аватар канала (файл). |

## API: боты и рассылки

| Метод | Путь | Описание |
| --- | --- | --- |
| GET | `/api/bots` | Список ботов. |
| GET | `/api/bots/{id}` | Карточка бота. |
| GET | `/api/broadcasts` | Список рассылок. |

## API: медиа-каталоги

| Метод | Путь | Описание |
| --- | --- | --- |
| GET | `/api/emoji` | Список emoji-наборов. |
| GET | `/api/emoji/{id}/animation` | Lottie-анимация emoji (файл). |
| GET | `/api/stickers` | Список стикерпаков. |
| GET | `/api/stickers/{id}/documents` | Документы стикерпака. |
| GET | `/api/stickers/documents/{id}/animation` | Анимация стикера (файл). |
| GET | `/api/gif-catalog` | Каталог GIF. |
| GET | `/api/gif-catalog/documents/{id}/preview` | Превью GIF (файл). |

## API: аудит сообщений

| Метод | Путь | Описание |
| --- | --- | --- |
| GET | `/api/messages` | Личные сообщения. Параметры: `owner_user_id`, `peer_id`, `before_date`, `before_id`, `limit`. |
| GET | `/api/messages/detail` | Детали личного сообщения. Параметры: `owner_user_id`, `msg_id`. |
| GET | `/api/messages/groups` | Сообщения групп/каналов. |
| GET | `/api/messages/groups/detail` | Детали сообщения в группе. Параметры: `channel_id`, `msg_id`. |

## API: звёздные подарки и коллекционки

| Метод | Путь | Описание |
| --- | --- | --- |
| GET | `/api/gifts` | Каталог звёздных подарков. |
| GET | `/api/gifts/{id}/animation` | Анимация подарка (файл). |
| GET | `/api/gifts/{id}/collectibles` | Коллекционные предметы подарка. |
| GET | `/api/gifts/{id}/collectibles/{kind}/{attribute_id}/animation` | Анимация атрибута коллекционки (файл). |
| GET | `/api/official-gifts` | Официальный каталог звёздных подарков. |
| GET | `/api/official-gifts/{id}/animation` | Анимация официального подарка (файл). |
| GET | `/api/collectible-usernames` | Коллекционные юзернеймы. |
| GET | `/api/collectible-usernames/{id}` | Карточка коллекционного юзернейма. |
| GET | `/api/collectible-phones` | Анонимные номера. |
| GET | `/api/collectible-phones/{id}` | Карточка анонимного номера. |

## API: Premium

Все маршруты раздела требуют права `premium.manage` (проверяется и на уровне
панели, и на стороне Admin API).

| Метод | Путь | Описание |
| --- | --- | --- |
| GET | `/api/premium/plans` | Каталог Premium-планов (проксируется в Admin API). |

## API: модерация

| Метод | Путь | Описание |
| --- | --- | --- |
| GET | `/api/moderation/cases` | Список кейсов модерации. |
| GET | `/api/moderation/cases/{id}` | Кейс модерации. |
| GET | `/api/moderation/reports/{id}` | Жалоба. |
| POST | `/api/moderation/cases/{id}/claim` | Взять кейс на себя. |
| POST | `/api/moderation/cases/{id}/decide` | Решение по кейсу. |
| POST | `/api/moderation/cases/{id}/appeals/{appeal_id}/review` | Рассмотрение апелляции. |

## API: официальная верификация

Все маршруты требуют права `verification.review`. Чтения идут напрямую из
PostgreSQL, решения — всегда через Admin API (журнал команд, статусная машина,
оптимистичная блокировка).

| Метод | Путь | Описание |
| --- | --- | --- |
| GET | `/api/verification/applications` | Очередь заявок. Параметры: `status`, `target_type`, `reviewer`, `q`, `before_id`, `limit`. Ответ: `rows`, `has_more`, `next_before_id`. |
| GET | `/api/verification/applications/{id}` | Детали заявки: сама заявка, события, `applicant_controls_target`, `target_verified`. |
| GET | `/api/verification/counts` | Счётчики заявок по статусам. |
| POST | `/api/verification/applications/{id}/claim` | Взять заявку. |
| POST | `/api/verification/applications/{id}/approve` | Одобрить заявку (выдать бейдж). |
| POST | `/api/verification/applications/{id}/reject` | Отклонить заявку. |
| POST | `/api/actions/revoke-verification` | Снять бейдж. Требует `verification.review` **и** `verification.revoke`. Адресует цель (тело: `target_type`, `target_id`), а не заявку. |

Общее тело решений: `command_id`, `reason`, `confirm`, `version` (токен
оптимистичной блокировки), `internal_note` (виден только оператору). Конфликт
«решил другой модератор» возвращается как `409 Conflict`.

## API: сторонняя (ботовая) верификация

Отдельный механизм с отдельными таблицами, правами и маршрутами. Чтения и
решения очереди требуют `botverification.review`; управление верификаторами,
каталогом иконок и снятие меток — `botverification.manage`.

| Метод | Путь | Описание |
| --- | --- | --- |
| GET | `/api/botverification/verifiers` | Список верификаторов. Параметры: `enabled_only`, `limit`. |
| GET | `/api/botverification/icons` | Каталог иконок верификаторов. |
| GET | `/api/botverification/marks` | Выданные метки (custom verifications). |
| GET | `/api/botverification/requests` | Очередь запросов на метку. |
| GET | `/api/botverification/requests/{id}` | Детали запроса. |
| GET | `/api/botverification/counts` | Счётчики запросов по статусам. |
| POST | `/api/botverification/requests/{id}/approve` | Одобрить запрос. |
| POST | `/api/botverification/requests/{id}/reject` | Отклонить запрос. |
| POST | `/api/botverification/requests/{id}/revoke` | Снять выданную метку по запросу. |
| POST | `/api/actions/grant-bot-verifier` | Назначить бота верификатором. Требует `botverification.manage`. |
| POST | `/api/actions/set-bot-verifier-enabled` | Включить/отключить верификатора. Требует `botverification.manage`. |
| POST | `/api/actions/revoke-bot-verifier` | Лишить бота статуса верификатора. Требует `botverification.manage`. |
| POST | `/api/actions/upsert-verification-icon` | Добавить/изменить иконку в каталоге. Требует `botverification.manage`. |
| POST | `/api/actions/set-verification-icon-active` | Включить/отключить иконку. Требует `botverification.manage`. |
| POST | `/api/actions/revoke-custom-verification` | Снять стороннюю метку. Требует `botverification.manage`. |

## API: действия над аккаунтами

Все маршруты — `POST`, требуют сессию и CSRF. Общие поля команд:
`command_id`, `reason`, `confirm`.

| Путь | Описание |
| --- | --- |
| `/api/actions/set-frozen` | Заморозить/разморозить аккаунт. |
| `/api/actions/grant-premium` | Выдать Premium. Требует `premium.manage`. |
| `/api/actions/upsert-premium-plan` | Создать/изменить Premium-план. Требует `premium.manage`. |
| `/api/actions/grant-stars` | Начислить звёзды. |
| `/api/actions/set-verified` | Поставить/снять официальный бейдж аккаунту. |
| `/api/actions/set-account-flags` | Флаги scam/fake аккаунта. |
| `/api/actions/set-support` | Пометить аккаунт как поддержку. |
| `/api/actions/set-account-username` | Сменить юзернейм аккаунта. |
| `/api/actions/set-account-profile` | Сменить профиль (имя, био, ...). |
| `/api/actions/set-account-phone` | Сменить номер телефона. |
| `/api/actions/set-account-login-email` | Сменить e-mail входа. |
| `/api/actions/set-account-avatar` | Сменить аватар (multipart). |
| `/api/actions/set-account-color` | Сменить цвет аккаунта. |
| `/api/actions/set-account-emoji-status` | Сменить emoji-статус. |
| `/api/actions/revoke-sessions` | Сбросить сессии аккаунта. |

## API: действия над каналами

| Путь | Описание |
| --- | --- |
| `/api/actions/set-channel-flags` | Флаги scam/fake канала. |
| `/api/actions/set-channel-settings` | Настройки канала. |
| `/api/actions/set-channel-username` | Сменить юзернейм канала. |
| `/api/actions/set-channel-color` | Сменить цвет канала. |
| `/api/actions/set-channel-emoji-status` | Сменить emoji-статус. |
| `/api/actions/set-channel-avatar` | Сменить аватар (multipart). |
| `/api/actions/set-channel-verified` | Поставить/снять официальный бейдж каналу. |

## API: действия над ботами

| Путь | Описание |
| --- | --- |
| `/api/actions/create-bot` | Создать бота. |
| `/api/actions/delete-bot` | Удалить бота. |
| `/api/actions/export-bot-token` | Экспорт токена бота. Требует `bots.token.read`. |
| `/api/actions/create-broadcast` | Создать рассылку. |

## API: действия над стикерами и emoji

| Путь | Описание |
| --- | --- |
| `/api/actions/create-sticker-set` | Создать стикерпак. |
| `/api/actions/rename-sticker-set` | Переименовать стикерпак. |
| `/api/actions/add-sticker-to-set` | Добавить стикер в набор. |
| `/api/actions/remove-sticker-from-set` | Убрать стикер из набора. |
| `/api/actions/set-sticker-set-archived` | Архивировать/разархивировать набор. |
| `/api/actions/set-sticker-set-sort-order` | Изменить порядок набора. |
| `/api/actions/delete-sticker-set` | Удалить стикерпак. |

## API: действия над GIF-каталогом

| Путь | Описание |
| --- | --- |
| `/api/actions/create-gif-catalog-entry` | Добавить запись в каталог. |
| `/api/actions/set-gif-catalog-enabled` | Включить/отключить запись. |
| `/api/actions/set-gif-catalog-sort-order` | Изменить порядок записи. |
| `/api/actions/delete-gif-catalog-entry` | Удалить запись. |

## API: действия над подарками и коллекционками

| Путь | Описание |
| --- | --- |
| `/api/actions/import-gift` | Импортировать звёздный подарок. |
| `/api/actions/import-official-gift` | Импортировать официальный подарок. |
| `/api/actions/publish-gift-collectibles` | Опубликовать коллекционки подарка. |
| `/api/actions/set-gift-enabled` | Включить/отключить подарок. |
| `/api/actions/set-gift-sort-order` | Изменить порядок подарка. |
| `/api/actions/give-gift` | Выдать подарок аккаунту. |

## API: действия над коллекционными юзернеймами

| Путь | Описание |
| --- | --- |
| `/api/actions/mint-collectible-username` | Выпустить коллекционный юзернейм. |
| `/api/actions/transfer-collectible-username` | Передать юзернейм другому аккаунту. |
| `/api/actions/revoke-collectible-username` | Отозвать юзернейм. |
| `/api/actions/delete-collectible-username` | Удалить юзернейм. |

## API: действия над анонимными номерами

| Путь | Описание |
| --- | --- |
| `/api/actions/mint-collectible-phone` | Выпустить анонимный номер. |
| `/api/actions/update-collectible-phone-price` | Обновить цену номера (USD/TON). |
| `/api/actions/transfer-collectible-phone` | Передать номер другому аккаунту. |
| `/api/actions/revoke-collectible-phone` | Отозвать номер (с `burn: true` — сжечь). |
| `/api/actions/delete-collectible-phone` | Удалить номер. |

## API: действия над рейтингом аккаунтов

| Путь | Описание |
| --- | --- |
| `/api/actions/recompute-account-rating` | Пересчитать рейтинг аккаунта. |
| `/api/actions/adjust-account-rating` | Скорректировать рейтинг вручную. |

## API: удаление сообщений

| Путь | Описание |
| --- | --- |
| `/api/actions/delete-messages` | Удалить конкретные сообщения. |
| `/api/actions/delete-history` | Очистить историю чата/канала. |

## Соглашения ответов

- Успешные чтения — `200` + JSON; файлы (аватары, анимации, превью) — сам файл.
- Ошибки — JSON вида `{"error": "...", "code": "..."}` с соответствующим HTTP-статусом.
- `401` — нет/просрочена сессия; `403` — нарушение CSRF/Origin или не хватает права
  (в `requirePermission` к телу добавляется поле `permission`);
  `409` — конфликт оптимистичной блокировки (кейс модерации, верификация);
  `502` — Admin API недоступен.
- Ошибки команд, ушедших в Admin API, возвращаются как
  `{"status": ..., "message": ..., "error": ...}`.