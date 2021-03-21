from dataclasses import asdict, dataclass, fields
from datetime import datetime, timedelta
from typing import List, Optional


@dataclass
class GenericJsonObject:
    id: int
    created_at: datetime
    updated_at: datetime
    update_count: int = 0
    nulled_on_evens: Optional[int] = None


@dataclass
class GenericJsonObjectDataSource:
    first_created_at: datetime
    objects_per_period_modulo: int
    update_frequency_period: int
    update_modulo: int
    random_seed: int = 123456789
    period_seconds: int = 60

    def periods_passed(self, dt: datetime) -> int:
        return int((dt - self.first_created_at).total_seconds() / self.period_seconds)

    def build_all_objects(self, now: datetime) -> List[GenericJsonObject]:
        curr_dt = self.first_created_at
        objects = []
        id_ = 1
        while curr_dt < now:
            periods_passed = self.periods_passed(curr_dt)
            num_objects = periods_passed % self.objects_per_period_modulo
            for i in range(num_objects):
                objects.append(
                    GenericJsonObject(
                        id=id_,
                        created_at=curr_dt,
                        updated_at=curr_dt,
                    )
                )
                id_ += 1
            curr_dt += timedelta(seconds=self.period_seconds)
        return objects

    def num_created_at_time(self, now: datetime) -> int:
        periods_passed = self.periods_passed(now)
        objects_created = periods_passed * (self.objects_per_period_modulo - 1) // 2
        return objects_created

    # def created_at(self, id: int, now: datetime) -> int:
    #     minutes_passed = int((now - self.first_created_at).total_seconds() / 60)
    #     objects_created = minutes_passed * (self.objects_per_minute_modulo - 1) // 2
    #     return objects_created

    # def update_count_at_time(self, id: int, now: datetime) -> int:
    #     minutes_passed = int((now - self.first_created_at).total_seconds() / 60)
    #     objects_created = minutes_passed * (self.objects_per_minute_modulo - 1) // 2
    #     return objects_created

    # def get_object_at_time(self, id: int, now: datetime) -> Optional[GenericJsonObject]:
    #     return GenericJsonObject(
    #         id=id,
    #         created_at=created_at,
    #         updated_at=updated_at,
    #         update_count=update_count,
    #         nulled_on_evens=update_count if update_count % 2 == 1 else None,
    #     )

    def get_by_page(
        self,
        now: datetime,
        limit: int = 100,
        page: int = 1,
        min_created_at: datetime = None,
        max_created_at: datetime = None,
        min_updated_at: datetime = None,
        max_updated_at: datetime = None,
        order_by: str = "created_at",
        order_direction: str = "asc",
    ) -> List[GenericJsonObject]:
        all_objects = self.build_all_objects(now)
        sorted_objects = sorted(all_objects, key=lambda x: getattr(x, order_by))
        if order_direction == "desc":
            sorted_objects = reversed(sorted_objects)
        filtered_objects = []
        for o in sorted_objects:
            if min_created_at and min_created_at > o.created_at:
                continue
            if max_created_at and max_created_at < o.created_at:
                continue
            if min_updated_at and min_updated_at > o.updated_at:
                continue
            if max_updated_at and max_updated_at < o.updated_at:
                continue
            filtered_objects.append(o)
        objects = filtered_objects[(limit * (page - 1)) : (limit * page)]
        return objects
