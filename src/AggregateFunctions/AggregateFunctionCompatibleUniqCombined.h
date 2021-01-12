//
// Created by Feng Guangyuan on 2020/12/22.
//

#ifndef CLICKHOUSE_AGGREGATEFUNCTIONCOMPATIBLEUNIQCOMBINED_H
#    define CLICKHOUSE_AGGREGATEFUNCTIONCOMPATIBLEUNIQCOMBINED_H

#endif //CLICKHOUSE_AGGREGATEFUNCTIONCOMPATIBLEUNIQCOMBINED_H

template <typename Key, UInt8 K>
struct AggregateFunctionUniqCombinedDataWithKey
{
    // TODO(ilezhankin): pre-generate values for |UniqCombinedBiasData|,
    //                   at the moment gen-bias-data.py script doesn't work.

    // We want to migrate from |HashSet| to |HyperLogLogCounter| when the sizes in memory become almost equal.
    // The size per element in |HashSet| is sizeof(Key)*2 bytes, and the overall size of |HyperLogLogCounter| is 2^K * 6 bits.
    // For Key=UInt32 we can calculate: 2^X * 4 * 2 ≤ 2^(K-3) * 6 ⇒ X ≤ K-4.
    using Set = CombinedCardinalityEstimator<Key, HashSet<Key, TrivialHash, UniqCombinedHashTableGrower>, 16, K - 5 + (sizeof(Key) == sizeof(UInt32)), K, TrivialHash, Key>;

    Set set;
};

template <typename T, UInt8 K, typename HashValueType>
struct AggregateFunctionCompatibleUniqCombinedData : public AggregateFunctionUniqCombinedDataWithKey<HashValueType, K>
{
};


/// For String keys, 64 bit hash is always used (both for uniqCombined and uniqCombined64),
///  because of backwards compatibility (64 bit hash was already used for uniqCombined).
template <UInt8 K, typename HashValueType>
struct AggregateFunctionCompatibleUniqCombinedData<String, K, HashValueType> : public AggregateFunctionUniqCombinedDataWithKey<UInt64 /*always*/, K>
{
};

template <typename T, UInt8 K>
class AggregateFunctionCompatibleUniqCombined final :
    public IAggregateFunctionDataHelper<AggregateFunctionUniqCombinedData<T, K, HashValueType>, AggregateFunctionCompatibleUniqCombined<T, K, HashValueType>>
{
public:
    AggregateFunctionCompatibleUniqCombined(const DataTypes & argument_types_, const Array & params_)
        : IAggregateFunctionDataHelper<
        AggregateFunctionCompatibleUniqCombinedData<T, K, HashValueType>,
        AggregateFunctionCompatibleUniqCombined<T, K, HashValueType>>(argument_types_, params_)
    {
    }

    String getName() const override
    {
        return "uniqCombinedCompat64";
    }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (!std::is_same_v<T, String>)
        {
            const auto & value = assert_cast<const ColumnVector<T> &>(*columns[0]).getElement(row_num);
            this->data(place).set.insert(detail::AggregateFunctionUniqCombinedTraits<T, HashValueType>::hash(value));
        }
        else
        {
            StringRef value = columns[0]->getDataAt(row_num);
            this->data(place).set.insert(CityHash_v1_0_2::CityHash64(value.data, value.size));
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).set.merge(this->data(rhs).set);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override { this->data(place).set.write(buf); }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override { this->data(place).set.read(buf); }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
    }
};