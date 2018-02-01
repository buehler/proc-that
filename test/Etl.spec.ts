import { Observable } from 'rxjs';

import { Etl, EtlState, Extractor, JsonExtractor, Loader, MatchMergeTransformer, Transformer } from '../src';

describe('Etl', () => {

    let etl: Etl;
    let extractor: Extractor = new JsonExtractor('./test/.testdata/json-extractor.object.json');
    let arrayExtractor: Extractor = new JsonExtractor('./test/.testdata/json-extractor.array.json');
    let matchMergeExtractor: Extractor = new JsonExtractor('./test/.testdata/match-merge.json');
    let o;
    let dummyExtractor: Extractor;
    let dummyTransformer: Transformer;
    let dummyLoader: Loader;

    beforeEach(() => {
        etl = new Etl();

        o = {_id: "001"};

        dummyExtractor = {
            read: () => Observable.of(o),
        };
        dummyExtractor.read = jest.fn(dummyExtractor.read);

        dummyTransformer = {
            process: o => Observable.of(o),
        };
        dummyTransformer.process = jest.fn(dummyTransformer.process);

        dummyLoader = {
            write: o => Observable.of(o),
        };
        dummyLoader.write = jest.fn(dummyLoader.write);

    });

    it('should initialize with correct default params', () => {
        expect(etl.state).toBe(EtlState.Stopped);
        expect(etl.extractors.length).toBe(0);
        expect(etl.transformers.length).toBe(0);
        expect(etl.loaders.length).toBe(0);
    });

    it('should reset correctly', () => {
        etl.addExtractor({
            read: function() {
                return null;
            }
        });

        expect(etl.extractors.length).toBe(1);
        etl.reset();
        expect(etl.extractors.length).toBe(0);
    });

    it('should pass context down the pipeline', done => {
        const context = 1;
        etl = new Etl(context);
        etl
            .addExtractor(dummyExtractor)
            .addTransformer(dummyTransformer)
            .addLoader(dummyLoader)
            .start()
            .subscribe(null, null, () => {
                expect((dummyExtractor.read as any).mock.calls[0]).toContain(context);
                expect((dummyTransformer.process as any).mock.calls[0]).toContain(context);
                expect((dummyLoader.write as any).mock.calls[0]).toContain(context);
                done();
            });
    });

    it('should pass newly set context down the pipeline', done => {
        const context = 1;
        etl
            .addExtractor(dummyExtractor)
            .addTransformer(dummyTransformer)
            .addLoader(dummyLoader)
            .setContext(context)
            .start()
            .subscribe(null, null, () => {
                expect((dummyExtractor.read as any).mock.calls[0]).toContain(context);
                expect((dummyTransformer.process as any).mock.calls[0]).toContain(context);
                expect((dummyLoader.write as any).mock.calls[0]).toContain(context);
                done();
            });
    });

    it('should process simple object', done => {
        etl
            .addExtractor(extractor)
            .addLoader(dummyLoader)
            .start()
            .subscribe(null, null, () => {
                expect((dummyLoader.write as any).mock.calls[0][0]).toMatchObject({ foo: 'bar', hello: 'world' });
                done();
            });
    });

    it('should process simple array', done => {
        etl
            .addExtractor(arrayExtractor)
            .addLoader(dummyLoader)
            .start()
            .subscribe(null, null, () => {
                expect((dummyLoader.write as any).mock.calls[0][0]).toMatchObject({ objId: 1, name: 'foobar' });
                expect((dummyLoader.write as any).mock.calls[1][0]).toMatchObject({ objId: 2, name: 'hello world' });
                expect((dummyLoader.write as any).mock.calls[2][0]).toMatchObject({ objId: 3, name: 'third test' });
                done();
            });
    });

    it('should call error on extractor error', done => {
        etl
            .addExtractor({
                read: () => Observable.throw(new Error('test'))
            })
            .addLoader(dummyLoader)
            .start()
            .subscribe(null, () => {
                done();
            }, () => {
                done(new Error('did not throw'));
            });
    });

    it('should call error on loader error', done => {
        etl
            .addExtractor(extractor)
            .addLoader({
                write: o => Observable.throw(new Error('test'))
            })
            .start()
            .subscribe(null, () => {
                done();
            }, () => {
                done(new Error('did not throw'));
            });
    });

    it('should call error on transformer error', done => {
        etl
            .addExtractor(extractor)
            .addLoader(dummyLoader)
            .addTransformer({
                process: o => Observable.throw(new Error('test'))
            })
            .start()
            .subscribe(null, () => {
                done();
            }, () => {
                done(new Error('did not throw'));
            });
    });

    it('should process simple object with transformer', done => {
        let spy = jest.fn();
        etl
            .addExtractor(extractor)
            .addLoader(dummyLoader)
            .addTransformer({
                process: o => Observable.of(o)
            })
            .start()
            .subscribe(spy, () => {
                done(new Error('did throw'));
            }, () => {
                expect(spy.mock.calls.length).toBe(1);
                done();
            });
    });

    it('should process simple array with transformer (flat)', done => {
        let spy = jest.fn();
        etl
            .addExtractor(arrayExtractor)
            .addLoader(dummyLoader)
            .addTransformer({
                process: o => Observable.from([o, o])
            })
            .start()
            .subscribe(spy, () => {
                done(new Error('did throw'));
            }, () => {
                expect(spy.mock.calls.length).toBe(6);
                done();
            });
    });

    it('should process a general transformer', done => {
        let spy = jest.fn();
        etl
            .addExtractor(arrayExtractor)
            .addLoader(dummyLoader)
            .addGeneralTransformer({
                process: o => o.reduce((x, y) => x + y.objId, 0)
            })
            .start()
            .subscribe(spy, () => {
                done(new Error('did throw'));
            }, () => {
                expect(spy.mock.calls.length).toBe(1);
                expect(spy.mock.calls[0][0]).toBe(6);
                done();
            });
    });

    it('should process a match-merge transformer', done => {
        let spy = jest.fn();

        class TestMatchTransformer extends MatchMergeTransformer {
            match(o1, o2) {
                return o1.location === o2.location;
            }

            merge(o1, o2) {
                return {
                    location: o1.location,
                    things: [...o1.things, ...o2.things]
                };
            }
        }

        etl
            .addExtractor(matchMergeExtractor)
            .addLoader(dummyLoader)
            .addGeneralTransformer(new TestMatchTransformer)
            .start()
            .subscribe(spy, () => {
                done(new Error('did throw'));
            }, () => {
                expect(spy.mock.calls.length).toBe(2);
                expect(spy.mock.calls[0][0]).toMatchObject({
                    location: "A",
                    things: ["a", "c"]
                });
                expect(spy.mock.calls[1][0]).toMatchObject({
                    location: "B",
                    things: ["b", "d"]
                });
                done();
            });
    });

    it('should pipe inital observable', done => {
        const context = 1;
        etl = new Etl(context);
        etl
            .addTransformer(dummyTransformer)
            .addLoader(dummyLoader)
            .start(Observable.of('hi'))
            .subscribe(null, null, () => {
                expect((dummyTransformer.process as any).mock.calls[0]).toContain('hi');
                expect((dummyLoader.write as any).mock.calls[0]).toContain('hi');
                done();
            });
    });

});
