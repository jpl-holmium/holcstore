class Range:

    def __init__(self, sd, ed):
        assert sd <= ed
        self.sd = sd
        self.ed = ed

    def __hash__(self):
        return hash(f'{self.sd}-{self.ed}')

    def __str__(self):
        return f'Interval from {self.sd} to {self.ed}'

    def __eq__(self, other):
        return (self.sd == other.sd) and (self.ed == other.ed)

    def overlap(self, interval):
        return (self.sd <= interval.ed) and (self.ed >= interval.sd)

    def check_date(self, date):
        """
        Check if date is in range
        """
        return (date <= self.ed) and (date >= self.sd)

    @staticmethod
    def sort_by_start(ranges, reverse=False):
        ranges.sort(key=lambda r: r.sd, reverse=reverse)

    @staticmethod
    def combine(ranges):
        Range.sort_by_start(ranges)
        itr = iter(ranges)
        current_range = next(itr, None)
        combined_ranges = []
        if current_range is not None:
            combined_ranges.append(current_range)
            next_range = next(itr, None)
            while next_range is not None:
                if combined_ranges[-1].overlap(next_range):
                    combined_ranges[-1].ed = next_range.ed
                else:
                    combined_ranges.append(next_range)
                next_range = next(itr, None)
        return combined_ranges

    def includes(self, interval):
        """
        Check if it is included in range
        """
        return self.check_date(interval.sd) and self.check_date(interval.ed)

    def intersection(self, interval, min_delta):
        start = max(interval.sd, self.sd)
        end = min(interval.ed, self.ed)
        if end - start >= min_delta:
            return Range(start, end)

    def difference_missing(self, interval, min_delta):
        result = []
        if self.overlap(interval):
            if self.includes(interval):
                return result
            elif interval.includes(self):
                if (self.sd - interval.sd) >= min_delta:
                    result.append(Range(interval.sd, self.sd))
                if (interval.ed - self.ed) >= min_delta:
                    result.append(Range(self.ed, interval.ed))
                return result
            else:
                pass
        if interval.ed > self.ed:
            if (interval.ed - self.ed) >= min_delta:
                return [Range(self.ed, interval.ed)]
        elif interval.sd < self.sd:
            if (self.sd - interval.sd) >= min_delta:
                return [Range(interval.sd, self.sd)]
        else:
            pass
        return result
