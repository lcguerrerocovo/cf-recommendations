from mrjob.job import MRJob
from math import sqrt
from itertools import combinations

def correlation(size, dot_product, rating_sum, \
            rating2sum, rating_norm_squared, rating2_norm_squared):
        numerator = size * dot_product - rating_sum * rating2sum
        denominator = sqrt(size * rating_norm_squared - rating_sum * rating_sum) * \
                        sqrt(size * rating2_norm_squared - rating2sum * rating2sum)

        return (numerator / (float(denominator))) if denominator else 0.0

def normalized_correlation(size, dot_product, rating_sum, \
                rating2sum, rating_norm_squared, rating2_norm_squared):
        similarity = correlation(size, dot_product, rating_sum, \
                rating2sum, rating_norm_squared, rating2_norm_squared)

        return (similarity + 1.0) / 2.0


class UserRecommendation(MRJob):

    def steps(self):
        return [
            self.mr(mapper=self.group_by_user_rating,
                    reducer=self.count_ratings_users_freq),
            self.mr(mapper=self.group_by_products_with_all_ratings,
                    reducer=self.fill_missing_implicit_ratings),
            self.mr(mapper=self.switch_to_user_ids,
                    reducer=self.flatten),
            self.mr(mapper=self.pairwise_items,
                    reducer=self.calculate_similarity),
            self.mr(mapper=self.calculate_ranking,
                    reducer=self.top_similar_items)]


    def group_by_user_rating(self, key, line):
        property_id, user_id, type, time = line.split(',')
        if type.lower().startswith("property contacted"):
            rating = 5.0
        else:
            rating = 1.0

        yield  user_id, (property_id, float(rating))


    def count_ratings_users_freq(self, user_id, values):
        item_count = 0
        rating_sum = 0
        property_rating_list = {}
        for property_id, rating in values:
            item_count += 1
            rating_sum += rating
            property_rating_list[property_id] = rating

        yield user_id, (item_count, rating_sum, property_rating_list)


    def group_by_products_with_all_ratings(self, user_id, values):
        item_count, rating_sum, property_rating_list = values

        for property_id in [i[0] for i in property_rating_list]:
            yield  property_id, (user_id, property_rating_list)


    def fill_missing_implicit_ratings(self, property_id, line):
        l = []
        for item in line:
            user_id, property_rating_list = item
            d = {x: 0.0 for x in property_rating_list}
            l.append(item)

        d_final = {}
        for user_id,property_rating_list in l:
            keys_a = set(d.keys())
            keys_b = set(property_rating_list.keys())
            intersection = keys_a - keys_b
            if not d_final.has_key(user_id):
                property_rating_list.update(dict.fromkeys(intersection, 0))
                d_final[user_id] = property_rating_list
        yield property_id,d_final.items()


    def switch_to_user_ids(self, property_id, values):
        for item in values:
            user_id , property_rating_list = item
            yield user_id , property_rating_list


    def flatten(self, user_id, property_ratings_list):
        for item in property_ratings_list:
            list = item
        yield user_id, list


    def pairwise_items(self, user_id, values):
        for item1, item2 in combinations(values.items(), 2):
            yield (item1[0], item2[0]), \
                    (item1[1], item2[1])


    def calculate_similarity(self, pair_key, lines):
        sum_xx, sum_xy, sum_yy, sum_x, sum_y, n = (0.0, 0.0, 0.0, 0.0, 0.0, 0)
        item_pair, co_ratings = pair_key, lines
        item_xname, item_yname = item_pair
        for item_x, item_y in lines:
            sum_xx += item_x * item_x
            sum_yy += item_y * item_y
            sum_xy += item_x * item_y
            sum_y += item_y
            sum_x += item_x
            n += 1
        similarity = normalized_correlation(n, sum_xy, sum_x, sum_y, \
                sum_xx, sum_yy)
        yield (item_xname, item_yname), (similarity, n)


    def calculate_ranking(self, item_keys, values):
        similarity, n = values
        item_x, item_y = item_keys
        if int(n) > 0:
            yield (item_x, similarity), (item_y, n)


    def top_similar_items(self, key_sim, similar_ns):
        item_x, similarity = key_sim
        for item_y, n in similar_ns:
            print '%s;%s;%f;%d' % (item_x, item_y, similarity, n)

if __name__ == '__main__':
    UserRecommendation.run()
