package com.ververica.flinktraining.exercises.datastream_java.process;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Java-реализация упражнения "Expiring State" для тренинга по Flink.
 * Цель: обогатить данные о поездках такси (TaxiRides) информацией о тарифах (TaxiFares).
 * Параметры:
 * -rides путь-к-файлу-данных-поездок
 * -fares путь-к-файлу-данных-тарифов
 */
public class ExpiringStateExercise extends ExerciseBase {
    // Определение боковых выходов для несопоставленных поездок и тарифов
    static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
    static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

    public static void main(String[] args) throws Exception {
        // Парсинг аргументов командной строки
        ParameterTool params = ParameterTool.fromArgs(args);
        final String ridesFile = params.get("rides", ExerciseBase.pathToRideData);
        final String faresFile = params.get("fares", ExerciseBase.pathToFareData);

        // Константы: максимальная задержка событий и скорость обработки
        final int maxEventDelay = 60;       // События могут быть неупорядочены до 60 секунд
        final int servingSpeedFactor = 600; // За 1 секунду обрабатывается 10 минут данных

        // Настройка среды выполнения потоковой обработки
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // Используем временные метки событий
        env.setParallelism(ExerciseBase.parallelism);                  // Устанавливаем параллелизм

        // Создание потока данных поездок с фильтрацией начальных поездок
        DataStream<TaxiRide> rides = env
                .addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))
                .filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0))) // Фильтр: только старты, исключая rideId кратные 1000
                .keyBy(ride -> ride.rideId); // Группировка по rideId

        // Создание потока данных тарифов
        DataStream<TaxiFare> fares = env
                .addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)))
                .keyBy(fare -> fare.rideId); // Группировка по rideId

        // Соединение и обработка потоков
        SingleOutputStreamOperator processed = rides
                .connect(fares) // Объединяем потоки поездок и тарифов
                .process(new EnrichmentFunction()); // Применяем функцию обогащения

        // Вывод несопоставленных тарифов (боковой выход)
        printOrTest(processed.getSideOutput(unmatchedFares));

        // Запуск программы
        env.execute("ExpiringStateSolution (java)");
    }

    /**
     * Функция совместной обработки для обогащения поездок данными о тарифах.
     * Использует состояние с истечением срока действия для управления несопоставленными данными.
     */
    public static class EnrichmentFunction 
            extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
        // Состояния для хранения поездок и тарифов
        private ValueState<TaxiRide> rideState;
        private ValueState<TaxiFare> fareState;

        @Override
        public void open(Configuration config) {
            // Инициализация состояний
            rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
            fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
        }

        @Override
        public void processElement1(TaxiRide ride, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) 
                throws Exception {
            // Обработка элемента поездки
            TaxiFare fare = fareState.value();
            if (fare != null) {
                // Если тариф уже есть, соединяем и очищаем состояние
                fareState.clear();
                context.timerService().deleteEventTimeTimer(fare.getEventTime());
                out.collect(new Tuple2<>(ride, fare));
            } else {
                // Если тарифа нет, сохраняем поездку и устанавливаем таймер
                rideState.update(ride);
                context.timerService().registerEventTimeTimer(ride.getEventTime());
            }
        }

        @Override
        public void processElement2(TaxiFare fare, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) 
                throws Exception {
            // Обработка элемента тарифа
            TaxiRide ride = rideState.value();
            if (ride != null) {
                // Если поездка уже есть, соединяем и очищаем состояние
                rideState.clear();
                context.timerService().deleteEventTimeTimer(ride.getEventTime());
                out.collect(new Tuple2<>(ride, fare));
            } else {
                // Если поездки нет, сохраняем тариф и устанавливаем таймер
                fareState.update(fare);
                context.timerService().registerEventTimeTimer(fare.getEventTime());
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) 
                throws Exception {
            // Обработка истечения таймера (водяного знака)
            if (fareState.value() != null) {
                // Если тариф остался несопоставленным, отправляем в боковой выход
                ctx.output(unmatchedFares, fareState.value());
                fareState.clear();
            }
            if (rideState.value() != null) {
                // Если поездка осталась несопоставленной, отправляем в боковой выход
                ctx.output(unmatchedRides, rideState.value());
                rideState.clear();
            }
        }
    }
}