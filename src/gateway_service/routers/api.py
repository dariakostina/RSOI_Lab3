import threading
from datetime import datetime
from functools import lru_cache, wraps
from queue import Queue
from typing import Annotated, Callable, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException

from gateway_service.circuit_breaker import CircuitBreaker
from gateway_service.clients.library_service_client import LibraryServiceClient
from gateway_service.clients.rating_service_client import RatingServiceClient
from gateway_service.clients.reservation_service_client import ReservationServiceClient
from gateway_service.models import (
    BookReservationResponse,
    ReturnBookRequest,
    TakeBookRequest,
    TakeBookResponse,
)
from gateway_service.settings import Settings
from reservation_service.models import (
    ReservationRequest,
    ReservationStatus,
)

router = APIRouter(prefix="/api/v1")

library_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout_sec=5,
    service_name="LibraryService",
    is_critical=True,  # для /libraries, /libraries/{uid}, /rating, /takeBook — критичный
)

reservation_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout_sec=5,
    service_name="ReservationService",
    is_critical=True,  # для /reservations — критично, без них вообще нет данных
)

rating_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout_sec=5,
    service_name="RatingService",
    is_critical=True,  # для /rating, /takeBook — критичен
)


@lru_cache(maxsize=1)
def get_library_service():
    return LibraryServiceClient(Settings().library_service_url)  # type: ignore


@lru_cache(maxsize=1)
def get_reservation_service():
    return ReservationServiceClient(Settings().reservation_service_url)  # type: ignore


@lru_cache(maxsize=1)
def get_rating_service():
    return RatingServiceClient(Settings().rating_service_url)  # type: ignore


LibraryServiceDep = Annotated[LibraryServiceClient, Depends(get_library_service)]
ReservationServiceDep = Annotated[
    ReservationServiceClient, Depends(get_reservation_service)
]
RatingServiceDep = Annotated[RatingServiceClient, Depends(get_rating_service)]

RETRY_QUEUE = Queue()


def process_retry_queue():
    max_items = 50

    for _ in range(min(max_items, RETRY_QUEUE.qsize())):
        try:
            task = RETRY_QUEUE.get_nowait()
        except Exception:
            break

        if not task:
            continue

        func = task["func"]
        args = task.get("args", [])
        kwargs = task.get("kwargs", {})

        try:
            func(*args, **kwargs)
        except Exception:
            RETRY_QUEUE.put(task)

    threading.Timer(10, process_retry_queue).start()

threading.Timer(10, process_retry_queue).start()


def fallback_503_library():
    raise HTTPException(status_code=503, detail="Library Service unavailable")


def fallback_503_reservation():
    raise HTTPException(status_code=503, detail="Reservation Service unavailable")


def fallback_503_rating():
    raise HTTPException(status_code=503, detail="Bonus Service unavailable")


def fallback_library_non_critical(bookUid: UUID, libraryUid: UUID):
    return {
        "bookUid": bookUid,
        "name": None,
        "author": None,
        "genre": None,
        "condition": None,
    }, {
        "libraryUid": libraryUid,
        "name": None,
        "city": None,
        "address": None,
    }


def circuit_breaker_read(
    breaker: CircuitBreaker,
    fallback: Optional[Callable] = None,
    fallback_args: Optional[tuple] = None,
):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return breaker.call(
                func=lambda: func(*args, **kwargs),
                fallback=(
                    lambda: fallback(*fallback_args)
                    if (fallback and fallback_args)
                    else fallback
                ),
            )

        return wrapper

    return decorator


# ---------------------------------------------------------
# 1) GET /api/v1/libraries -> прокси к Library Service (критичный сервис)
# ---------------------------------------------------------
@router.get("/libraries")
@circuit_breaker_read(library_breaker, fallback=fallback_503_library)
def gateway_libraries(
    library_client: LibraryServiceDep, city: str, page: int = 1, size: int = 10
):
    return library_client.get_libraries(city, page, size)


# ---------------------------------------------------------
# 2) GET /api/v1/libraries/{libraryUid}/books -> прокси к Library Service (критичный сервис)
# ---------------------------------------------------------
@router.get("/libraries/{libraryUid}/books")
@circuit_breaker_read(library_breaker, fallback=fallback_503_library)
def gateway_library_books(
    library_client: LibraryServiceDep,
    libraryUid: UUID,
    showAll: bool = False,
    page: int = 1,
    size: int = 10,
):
    return library_client.get_library_books(libraryUid, showAll, page, size)


# ---------------------------------------------------------
# 3) GET /api/v1/reservations -> прокси к Reservation Service (критично)
#    + Дополнительно внутри пытаемся сходить в LibraryService (некритично)
# ---------------------------------------------------------
@router.get("/reservations")
@circuit_breaker_read(reservation_breaker, fallback=fallback_503_reservation)
def gateway_reservations(
    reservation_client: ReservationServiceDep,
    library_client: LibraryServiceDep,
    x_user_name: str = Header(..., alias="X-User-Name"),
):
    data = reservation_client.get_reservations(x_user_name)
    result = []
    for x in data:
        try:
            book = library_breaker.call(
                func=lambda: library_client.get_library_book(x.libraryUid, x.bookUid),
                fallback=lambda: fallback_library_non_critical(x.bookUid, x.libraryUid)[
                    0
                ],
            )
            lib = library_breaker.call(
                func=lambda: library_client.get_library_info(x.libraryUid),
                fallback=lambda: fallback_library_non_critical(x.bookUid, x.libraryUid)[
                    1
                ],
            )
        except HTTPException:
            book = {"bookUid": x.bookUid}
            lib = {"libraryUid": x.libraryUid}

        result.append(
            BookReservationResponse(
                reservationUid=x.reservationUid,
                status=x.status,
                startDate=x.startDate,
                tillDate=x.tillDate,
                book=book,
                library=lib,
            )
        )
    return result


# ---------------------------------------------------------
# 6) GET /api/v1/rating -> прокси к Rating Service (критичный)
# ---------------------------------------------------------
@router.get("/rating")
@circuit_breaker_read(rating_breaker, fallback=fallback_503_rating)
def gateway_rating(
    rating_client: RatingServiceDep, x_user_name: str = Header(..., alias="X-User-Name")
):
    return rating_client.get_rating(x_user_name)


# ---------------------------------------------------------
# 4) POST /api/v1/reservations -> логика "взять книгу"
# ---------------------------------------------------------
@router.post("/reservations", response_model=TakeBookResponse)
def gateway_take_book(
    library_client: LibraryServiceDep,
    reservation_client: ReservationServiceDep,
    rating_client: RatingServiceDep,
    req: TakeBookRequest,
    x_user_name: str = Header(..., alias="X-User-Name"),
):
    """
    1) Запрос к LibraryService и RatingService, чтобы проверить возможность взять книгу.
       (оба сервисы критичны – при недоступности -> HTTP 503)
    2) Создать запись в ReservationService (тоже критичен).
    3) Уменьшить count книг в Library (критично).
    4) Если что-то упало в середине, откатываем шаги и делаем вид, что всё успешно (с постановкой в очередь).
    """

    compensate_steps = []

    def rollback_all():
        for step in reversed(compensate_steps):
            try:
                step()
            except Exception:
                pass

    # Шаг 1) проверяем доступность книги и лимит пользователя
    try:
        book_found = library_breaker.call(
            func=lambda: library_client.get_library_book(req.libraryUid, req.bookUid),
            fallback=fallback_503_library,
        )
        if not book_found:
            raise HTTPException(400, "Книга недоступна или не найдена.")

        reservations = reservation_breaker.call(
            func=lambda: reservation_client.get_reservations(x_user_name),
            fallback=fallback_503_reservation,
        )

        rating_resp = rating_breaker.call(
            func=lambda: rating_client.get_rating(x_user_name),
            fallback=fallback_503_rating,
        )

        rented_count = sum(
            1 for r in reservations if r.status == ReservationStatus.RENTED
        )
        user_rating = rating_resp.stars
        max_books = max(1, min(user_rating, 100))

        if rented_count >= max_books:
            raise HTTPException(
                400,
                f"Вы уже взяли {rented_count} книг из {max_books} доступных по вашему рейтингу.",
            )
    except Exception:
        raise

    # Шаг 2) Создаём запись в ReservationService
    now = datetime.now()
    reservation_body = ReservationRequest(
        reservationUid=None,
        username=x_user_name,
        bookUid=req.bookUid,
        libraryUid=req.libraryUid,
        status=ReservationStatus.RENTED,
        startDate=now,
        tillDate=req.tillDate,
    )
    try:
        created_res = reservation_breaker.call(
            func=lambda: reservation_client.create_reservation(
                x_user_name, reservation_body
            ),
            fallback=fallback_503_reservation,
        )
        if not created_res:
            raise HTTPException(503, "Не удалось создать запись бронирования.")
        compensate_steps.append(
            lambda: reservation_breaker.call(
                func=lambda: reservation_client.undo_return_book(
                    created_res.reservationUid, x_user_name
                ),
                fallback=None,
            )
        )
    except Exception:
        raise

    # Шаг 3) Уменьшаем available_count в Library
    try:
        library_breaker.call(
            func=lambda: library_client.reduce_book_count(req.libraryUid, req.bookUid),
            fallback=fallback_503_library,
        )
        compensate_steps.append(
            lambda: library_breaker.call(
                func=lambda: library_client.increase_book_count(
                    req.libraryUid, req.bookUid
                ),
                fallback=None,
            )
        )
    except Exception:
        rollback_all()
        _queue_task(
            gateway_take_book,
            (),
            {
                "library_client": library_client,
                "reservation_client": reservation_client,
                "rating_client": rating_client,
                "req": req,
                "x_user_name": x_user_name,
            },
        )
        return _fake_take_book_response(req)

    # Шаг 4) Формируем ответ — всё ок
    library_data = library_breaker.call(
        func=lambda: library_client.get_library_info(req.libraryUid),
        fallback=fallback_503_library,
    )
    rating_after = rating_breaker.call(
        func=lambda: rating_client.get_rating(x_user_name), fallback=fallback_503_rating
    )

    return TakeBookResponse(
        reservationUid=created_res.reservationUid,
        status=created_res.status,
        startDate=created_res.startDate,
        tillDate=created_res.tillDate,
        book=TakeBookResponse.BookResponse(
            bookUid=book_found.bookUid,
            name=book_found.name,
            author=book_found.author,
            genre=book_found.genre,
        ),
        library=library_data,
        rating=rating_after,
    )


def _fake_take_book_response(req: TakeBookRequest) -> dict:
    return {
        "reservationUid": None,
        "status": "RENTED",
        "startDate": str(datetime.now()),
        "tillDate": str(req.tillDate),
        "book": {
            "bookUid": str(req.bookUid),
            "name": None,
            "author": None,
            "genre": None,
        },
        "library": {
            "libraryUid": str(req.libraryUid),
            "name": None,
            "city": None,
            "address": None,
        },
        "rating": {"stars": 0},
    }


def _queue_task(func: Callable, args: tuple, kwargs: dict):
    RETRY_QUEUE.put({"func": func, "args": args, "kwargs": kwargs})


# ---------------------------------------------------------
# 5) POST /api/v1/reservations/{reservationUid}/return -> вернуть книгу
# ---------------------------------------------------------
@router.post("/reservations/{reservationUid}/return", status_code=204)
def gateway_return_book(
    library_client: LibraryServiceDep,
    reservation_client: ReservationServiceDep,
    rating_client: RatingServiceDep,
    reservationUid: UUID,
    req: ReturnBookRequest,
    x_user_name: str = Header(..., alias="X-User-Name"),
):
    """
    1) Ставим статус RETURNED или EXPIRED в Rental (Reservation) Service (критично).
    2) Увеличить available_count в Library (если упало, вернуть успех и поставить операцию в очередь).
    3) Изменить рейтинг пользователя (если упало, вернуть успех и поставить операцию в очередь).
    """

    compensate_steps = []

    def rollback_all():
        for step in reversed(compensate_steps):
            try:
                step()
            except Exception:
                pass

    # Шаг 1) Узнаём данные о бронировании + вызываем return_book
    try:
        rdata = reservation_breaker.call(
            func=lambda: reservation_client.get_reservation(
                reservationUid, x_user_name
            ),
            fallback=fallback_503_reservation,
        )
        if not rdata:
            raise HTTPException(404, "Reservation not found")

        return_resp = reservation_breaker.call(
            func=lambda: reservation_client.return_book(
                reservationUid, x_user_name, req.date
            ),
            fallback=fallback_503_reservation,
        )
        compensate_steps.append(
            lambda: reservation_breaker.call(
                func=lambda: reservation_client.undo_return_book(
                    reservationUid, x_user_name
                ),
                fallback=None,
            )
        )
        new_status = return_resp.status

        book_info = library_breaker.call(
            func=lambda: library_client.get_library_book(
                rdata.libraryUid, rdata.bookUid
            ),
            fallback=fallback_503_library,
        )
        old_condition = book_info.condition if book_info else "UNKNOWN"

        library_breaker.call(
            func=lambda: library_client.update_book_condition(
                rdata.libraryUid, rdata.bookUid, req.condition
            ),
            fallback=fallback_503_library,
        )
        compensate_steps.append(
            lambda: library_breaker.call(
                func=lambda: library_client.update_book_condition(
                    rdata.libraryUid, rdata.bookUid, old_condition
                ),
                fallback=None,
            )
        )
    except Exception:
        raise

    # Шаг 2) Увеличиваем available_count в Library
    try:
        library_breaker.call(
            func=lambda: library_client.increase_book_count(
                rdata.libraryUid, rdata.bookUid
            ),
            fallback=None,
        )
        compensate_steps.append(
            lambda: library_breaker.call(
                func=lambda: library_client.reduce_book_count(
                    rdata.libraryUid, rdata.bookUid
                ),
                fallback=None,
            )
        )
    except Exception:
        rollback_all()
        _queue_task(
            _retry_return_book,
            (),
            {
                "libraryUid": rdata.libraryUid,
                "bookUid": rdata.bookUid,
                "reservationUid": reservationUid,
                "condition": req.condition,
                "username": x_user_name,
                "date": req.date,
            },
        )
        return

    # Шаг 3) Корректируем рейтинг
    penalty = 0
    if new_status == ReservationStatus.EXPIRED:
        penalty += 10
    if req.condition != "EXCELLENT":
        penalty += 10

    try:
        if penalty > 0:
            rating_breaker.call(
                func=lambda: rating_client.decrease_rating(x_user_name, penalty),
                fallback=None,
            )
            compensate_steps.append(
                lambda: rating_breaker.call(
                    func=lambda: rating_client.increase_rating(x_user_name, penalty),
                    fallback=None,
                )
            )
        else:
            rating_breaker.call(
                func=lambda: rating_client.increase_rating(x_user_name, 1),
                fallback=None,
            )
            compensate_steps.append(
                lambda: rating_breaker.call(
                    func=lambda: rating_client.decrease_rating(x_user_name, 1),
                    fallback=None,
                )
            )
    except Exception:
        rollback_all()
        _queue_task(
            _retry_return_book,
            (),
            {
                "libraryUid": rdata.libraryUid,
                "bookUid": rdata.bookUid,
                "reservationUid": reservationUid,
                "condition": req.condition,
                "username": x_user_name,
                "date": req.date,
            },
        )
        return

    return


def _retry_return_book(
    libraryUid: UUID,
    bookUid: UUID,
    reservationUid: UUID,
    condition: str,
    username: str,
    date: datetime,
):
    # 1) Увеличить available_count
    # 2) Обновить рейтинг

    settings = Settings()
    library_client = LibraryServiceClient(settings.library_service_url)
    rating_client = RatingServiceClient(settings.rating_service_url)

    library_breaker.call(
        func=lambda: library_client.increase_book_count(libraryUid, bookUid),
        fallback=None,
    )

    penalty = 0
    if date > datetime.now():
        penalty += 10
    if condition != "EXCELLENT":
        penalty += 10

    if penalty > 0:
        rating_breaker.call(
            func=lambda: rating_client.decrease_rating(username, penalty), fallback=None
        )
    else:
        rating_breaker.call(
            func=lambda: rating_client.increase_rating(username, 1), fallback=None
        )
